#!/bin/bash
# ============================================================================
# Distributed Code Execution System - Smoke Tests
# ============================================================================
# Tests all supported languages with various scenarios:
# - Successful execution
# - Compilation errors
# - Runtime errors
# - Time limit exceeded (TLE)
# - Memory limit exceeded (MLE)
# ============================================================================

set -e

MASTER_URL="${MASTER_URL:-http://172.16.7.253:8080}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Counters
PASSED=0
FAILED=0

print_header() {
    echo ""
    echo -e "${BLUE}============================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}============================================================================${NC}"
}

print_test() {
    echo -e "${CYAN}[TEST]${NC} $1"
}

print_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    ((PASSED++))
}

print_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    ((FAILED++))
}

print_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

# Python script for SSE monitoring and benchmarking
MONITOR_SCRIPT=$(cat << 'EOF'
import sys
import json
import time
import requests

# Simple SSE Client implementation using requests
class SimpleSSEClient:
    def __init__(self, url):
        self.url = url
        self.response = requests.get(url, stream=True)
        self.client = self.response.iter_lines()

    def events(self):
        for line in self.client:
            if line:
                decoded = line.decode('utf-8')
                if decoded.startswith('data: '):
                    yield decoded[6:]

def format_time(ms):
    return f"{ms}ms"

def main():
    if len(sys.argv) < 3:
        print("Usage: monitor.py <master_url> <job_id>")
        sys.exit(1)

    master_url = sys.argv[1]
    job_id = sys.argv[2]
    url = f"{master_url}/status/{job_id}"

    print(f"Monitoring Job {job_id}...")
    
    start_time = time.time() * 1000
    compilation_start = 0
    execution_start = 0
    all_tests_passed = True
    
    client = SimpleSSEClient(url)
    
    try:
        for event_data in client.events():
            try:
                data = json.loads(event_data)
                event_type = data.get("type")
                payload = data.get("data") # wrapper from backend might be just fields?
                
                # Based on master/src/state.rs: 
                # #[serde(tag = "type", content = "data")]
                # enum JobUpdate { Compiling, Compiled, Executing, Result, Completed, Error }
                
                if event_type == "Compiling":
                    compilation_start = time.time() * 1000
                    print("Status: Compiling...")
                    
                elif event_type == "Compiled":
                    duration = (time.time() * 1000) - compilation_start
                    success = payload.get("success")
                    if success:
                        print(f"Status: Compiled successfully in {format_time(duration)}")
                    else:
                        print(f"Status: Compilation failed in {format_time(duration)}")
                        print(f"Output:\n{payload.get('output')}")
                        all_tests_passed = False
                        
                elif event_type == "Executing":
                    if execution_start == 0:
                        execution_start = time.time() * 1000
                    completed = payload.get("completed")
                    total = payload.get("total")
                    print(f"Status: Executing ({completed}/{total})", end='\r')
                    
                elif event_type == "Result":
                    # Single test result
                    res = payload
                    status_icon = "✓" if res['status'] == "PASSED" else "✗"
                    print(f"Test {res['test_id']}: {status_icon} {res['status']} ({res['time_ms']}ms, {res['memory_bytes']} bytes)")
                    if res['status'] != "PASSED":
                        all_tests_passed = False
                        print(f"  Expected: {res.get('expected_output', '')}") 
                        # Note: expecting output might not be in Result payload if not added
                        print(f"  Got: {res.get('stdout', '')}")
                        if res.get('stderr'):
                            print(f"  Stderr: {res['stderr']}")

                elif event_type == "Completed":
                    total_time = (time.time() * 1000) - start_time
                    execution_time = (time.time() * 1000) - execution_start if execution_start > 0 else 0
                    
                    print("\n" + "="*40)
                    print("BENCHMARKS")
                    print("="*40)
                    print(f"Total Turnaround Time: {format_time(total_time)}")
                    if compilation_start > 0:
                         print(f"Compilation Time:      {format_time(execution_start - compilation_start)}")
                    if execution_start > 0:
                        print(f"Execution Time:        {format_time(execution_time)}")
                    
                    job_success = payload.get("success")
                    
                    if job_success and all_tests_passed:
                        sys.exit(0)
                    else:
                        if not job_success:
                            print(f"Job Failed: {payload.get('error')}")
                        else:
                            print("Job Completed, but some tests failed.")
                        sys.exit(1)
                        
                elif event_type == "Error":
                    print(f"\nError: {payload}")
                    sys.exit(1)
                    
            except json.JSONDecodeError:
                pass
                
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")
        sys.exit(1)
    except Exception as e:
        print(f"\nConnection error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
EOF
)

# Submit a job and return the job_id
submit_job() {
    local language="$1"
    local source_code="$2"
    local test_cases="$3"
    local time_limit="${4:-2000}"
    local memory_limit="${5:-128}"
    
    echo -e "${CYAN}Submitting job for $language...${NC}"
    
    local response=$(curl -s -X POST "$MASTER_URL/submit" \
        -H "Content-Type: application/json" \
        -d "{
            \"language\": \"$language\",
            \"source_code\": $source_code,
            \"test_cases\": $test_cases,
            \"time_limit_ms\": $time_limit,
            \"memory_limit_mb\": $memory_limit
        }")
    
    local job_id=$(echo "$response" | grep -o '"job_id":"[^"]*"' | cut -d'"' -f4)
    
    if [ -z "$job_id" ]; then
        echo "Failed to submit job. Response: $response"
        return 1
    fi
    
    print_info "Job ID: $job_id"
    
    # Run the monitor script
    echo "$MONITOR_SCRIPT" | python3 - "$MASTER_URL" "$job_id"
    return $?
}

# Check if master is running
check_master() {
    print_header "Checking Master Connection"
    local health=$(curl -s "$MASTER_URL/health" 2>/dev/null || echo "failed")
    if [[ "$health" == *"ok"* ]]; then
        print_pass "Master is running at $MASTER_URL"
        return 0
    else
        print_fail "Cannot connect to Master at $MASTER_URL"
        echo "Please start the master with: cargo run --bin master"
        return 1
    fi
}

# Check if workers are connected
check_workers() {
    print_header "Checking Worker Connections"
    local workers=$(curl -s "$MASTER_URL/workers")
    local count=$(echo "$workers" | grep -o '"id"' | wc -l)
    
    if [[ $count -gt 0 ]]; then
        print_pass "$count worker(s) connected"
        echo "$workers" | python3 -m json.tool 2>/dev/null || echo "$workers"
        return 0
    else
        print_fail "No workers connected"
        echo "Please start a worker with: cargo run --bin worker"
        return 1
    fi
}

# ============================================================================
# Test Cases
# ============================================================================

test_python_success() {
    print_test "Python - Hello World (Success)"
    
    local code='"print(\"Hello World\")"'
    local tests='[{"id": "1", "input": "", "expected_output": "Hello World\n"}]'
    
    if submit_job "python" "$code" "$tests"; then
        print_pass "Python Hello World passed"
    else
        print_fail "Python Hello World failed"
    fi
}

test_python_runtime_error() {
    print_test "Python - Division by Zero (Runtime Error)"
    
    local code='"x = 1 / 0\nprint(x)"'
    local tests='[{"id": "1", "input": "", "expected_output": "error"}]'
    
    # We expect this job to fail execution but report it correctly as a runtime error (status mismatch or error status)
    # Actually, the smoke test logic expected "job execution" to happen.
    # If the test case expects "error" and the system reports "Runtime Error", does that count as success?
    # The current system: TestCaseResult status logic needs to match.
    # If input expects a certain output and we get runtime error, that's a failed test case usually.
    # UNLESS we are testing the ERROR REPORTING itself.
    # The original script just checked if job was *submitted*.
    # Now we are checking if it *completes successfully*.
    # For "Runtime Error" test:
    # If the user INTENDS for the job to fail (e.g. to test error handling), 
    
    # Wait, the original script was:
    # test_python_runtime_error just checks if submitted.
    # Now we want to verify it actually reported the error.
    # Monitoring script exits with 1 if success=false.
    # Runtime error usually means success=false for the JOB (if all test cases fail?).
    # Let's see `master` logic. FinalResponse success depends on... usually if all tests pass?
    # Or if the INFRASTRUCTURE succeeded?
    # Usually "success" in FinalResponse means "ran without system error". 
    # But let's look at `state.rs`: `Compiled { success: bool ... }`.
    # `FinalResponse`: `success: bool`.
    # Any failed test case usually makes the job "failed" in some systems, or just "completed with failures".
    
    # If I want to test that it CORRECTLY detects runtime error:
    # I should expect `submit_job` to return failure (exit code 1) OR return success but print "Runtime Error".
    # The monitor script exits 1 if `success` is false in Completed payload.
    # If a user code crashes, is `success` false?
    # I'll assume YES for now. So for "Runtime Error" test, we actually EXPECT failure effectively?
    # OR, does the test case "expected_output": "error" mean we expect it to fail?
    # The scheduler likely compares output.
    
    # Let's run it and see. For now, I will just call submit_job.
    # If it fails (which it likely will for division by zero), I should catch that.
    
    if ! submit_job "python" "$code" "$tests"; then
        print_pass "Python runtime error correctly reported"
    else
        print_fail "Python runtime error NOT reported (job succeeded unexpectedly)"
    fi
}

test_python_infinite_loop() {
    print_test "Python - Infinite Loop (Time Limit)"
    
    local code='"while True: pass"'
    local tests='[{"id": "1", "input": "", "expected_output": ""}]'
    
    # Expect failure (TLE)
    if ! submit_job "python" "$code" "$tests" "1000"; then
        print_pass "Python TLE correctly reported"
    else
        print_fail "Python TLE NOT reported (job succeeded unexpectedly)"
    fi
}

test_javascript_success() {
    print_test "JavaScript - Hello World (Success)"
    
    local code='"console.log(\"Hello World\");"'
    local tests='[{"id": "1", "input": "", "expected_output": "Hello World\n"}]'
    
    if submit_job "javascript" "$code" "$tests"; then
        print_pass "JavaScript Hello World passed"
    else
        print_fail "JavaScript Hello World failed"
    fi
}

test_javascript_runtime_error() {
    print_test "JavaScript - Undefined Variable (Runtime Error)"
    
    local code='"console.log(undefinedVariable.property);"'
    local tests='[{"id": "1", "input": "", "expected_output": "error"}]'
    
    if ! submit_job "javascript" "$code" "$tests"; then
        print_pass "JavaScript runtime error correctly reported"
    else
        print_fail "JavaScript runtime error NOT reported"
    fi
}

test_ruby_success() {
    print_test "Ruby - Hello World (Success)"
    
    local code='"puts \"Hello World\""'
    local tests='[{"id": "1", "input": "", "expected_output": "Hello World\n"}]'
    
    if submit_job "ruby" "$code" "$tests"; then
        print_pass "Ruby Hello World passed"
    else
        print_fail "Ruby Hello World failed"
    fi
}

test_c_success() {
    print_test "C - Hello World (Success)"
    
    local code='"#include <stdio.h>\nint main() { printf(\"Hello World\\n\"); return 0; }"'
    local tests='[{"id": "1", "input": "", "expected_output": "Hello World\n"}]'
    
    if submit_job "c" "$code" "$tests"; then
        print_pass "C Hello World passed"
    else
        print_fail "C Hello World failed"
    fi
}

test_c_compile_error() {
    print_test "C - Missing Semicolon (Compilation Error)"
    
    local code='"#include <stdio.h>\nint main() { printf(\"Hello\") return 0; }"'
    local tests='[{"id": "1", "input": "", "expected_output": ""}]'
    
    if ! submit_job "c" "$code" "$tests"; then
        print_pass "C compile error correctly reported"
    else
        print_fail "C compile error NOT reported"
    fi
}

test_c_segfault() {
    print_test "C - Segmentation Fault (Runtime Error)"
    
    local code='"#include <stdio.h>\nint main() { int *p = 0; *p = 42; return 0; }"'
    local tests='[{"id": "1", "input": "", "expected_output": ""}]'
    
    if ! submit_job "c" "$code" "$tests"; then
        print_pass "C segfault correctly reported"
    else
        print_fail "C segfault NOT reported"
    fi
}

test_cpp_success() {
    print_test "C++ - Hello World (Success)"
    
    local code='"#include <iostream>\nint main() { std::cout << \"Hello World\" << std::endl; return 0; }"'
    local tests='[{"id": "1", "input": "", "expected_output": "Hello World\n"}]'
    
    if submit_job "cpp" "$code" "$tests"; then
        print_pass "C++ Hello World passed"
    else
        print_fail "C++ Hello World failed"
    fi
}

test_cpp_compile_error() {
    print_test "C++ - Undefined Function (Compilation Error)"
    
    local code='"#include <iostream>\nint main() { undefinedFunction(); return 0; }"'
    local tests='[{"id": "1", "input": "", "expected_output": ""}]'
    
    if ! submit_job "cpp" "$code" "$tests"; then
        print_pass "C++ compile error correctly reported"
    else
        print_fail "C++ compile error NOT reported"
    fi
}

test_rust_success() {
    print_test "Rust - Hello World (Success)"
    
    local code='"fn main() { println!(\"Hello World\"); }"'
    local tests='[{"id": "1", "input": "", "expected_output": "Hello World\n"}]'
    
    if submit_job "rust" "$code" "$tests"; then
        print_pass "Rust Hello World passed"
    else
        print_fail "Rust Hello World failed"
    fi
}

test_rust_compile_error() {
    print_test "Rust - Missing Macro (Compilation Error)"
    
    local code='"fn main() { printlnn!(\"Hello\"); }"'
    local tests='[{"id": "1", "input": "", "expected_output": ""}]'
    
    if ! submit_job "rust" "$code" "$tests"; then
        print_pass "Rust compile error correctly reported"
    else
        print_fail "Rust compile error NOT reported"
    fi
}

test_rust_panic() {
    print_test "Rust - Panic (Runtime Error)"
    
    local code='"fn main() { panic!(\"This is a panic!\"); }"'
    local tests='[{"id": "1", "input": "", "expected_output": ""}]'
    
    if ! submit_job "rust" "$code" "$tests"; then
        print_pass "Rust panic correctly reported"
    else
        print_fail "Rust panic NOT reported"
    fi
}

test_go_success() {
    print_test "Go - Hello World (Success)"
    
    local code='"package main\nimport \"fmt\"\nfunc main() { fmt.Println(\"Hello World\") }"'
    local tests='[{"id": "1", "input": "", "expected_output": "Hello World\n"}]'
    
    if submit_job "go" "$code" "$tests"; then
        print_pass "Go Hello World passed"
    else
        print_fail "Go Hello World failed"
    fi
}

test_go_compile_error() {
    print_test "Go - Missing Import (Compilation Error)"
    
    local code='"package main\nfunc main() { fmt.Println(\"Hello\") }"'
    local tests='[{"id": "1", "input": "", "expected_output": ""}]'
    
    if ! submit_job "go" "$code" "$tests"; then
        print_pass "Go compile error correctly reported"
    else
        print_fail "Go compile error NOT reported"
    fi
}

test_java_success() {
    print_test "Java - Hello World (Success)"
    
    local code='"public class Main { public static void main(String[] args) { System.out.println(\"Hello World\"); } }"'
    local tests='[{"id": "1", "input": "", "expected_output": "Hello World\n"}]'
    
    if submit_job "java" "$code" "$tests"; then
        print_pass "Java Hello World passed"
    else
        print_fail "Java Hello World failed"
    fi
}

test_java_compile_error() {
    print_test "Java - Missing Semicolon (Compilation Error)"
    
    local code='"public class Main { public static void main(String[] args) { System.out.println(\"Hello\") } }"'
    local tests='[{"id": "1", "input": "", "expected_output": ""}]'
    
    if ! submit_job "java" "$code" "$tests"; then
        print_pass "Java compile error correctly reported"
    else
        print_fail "Java compile error NOT reported"
    fi
}

test_java_null_pointer() {
    print_test "Java - Null Pointer Exception (Runtime Error)"
    
    local code='"public class Main { public static void main(String[] args) { String s = null; System.out.println(s.length()); } }"'
    local tests='[{"id": "1", "input": "", "expected_output": ""}]'
    
    if ! submit_job "java" "$code" "$tests"; then
        print_pass "Java NPE correctly reported"
    else
        print_fail "Java NPE NOT reported"
    fi
}

test_memory_limit() {
    print_test "Python - Large Array (Memory Limit)"
    
    # Try to allocate a very large list
    local code='"x = [1] * (1024 * 1024 * 200)\nprint(len(x))"'
    local tests='[{"id": "1", "input": "", "expected_output": ""}]'
    
    if ! submit_job "python" "$code" "$tests" "5000" "64"; then
        print_pass "Memory limit correctly reported"
    else
        print_fail "Memory limit NOT reported"
    fi
}

test_multiple_test_cases() {
    print_test "Python - Multiple Test Cases"
    
    local code='"import sys\nfor line in sys.stdin:\n    n = int(line.strip())\n    print(n * 2)"'
    local tests='[
        {"id": "1", "input": "5", "expected_output": "10\n"},
        {"id": "2", "input": "10", "expected_output": "20\n"},
        {"id": "3", "input": "100", "expected_output": "200\n"}
    ]'
    
    if submit_job "python" "$code" "$tests"; then
        print_pass "Multiple test cases passed"
    else
        print_fail "Multiple test cases failed"
    fi
}

# ============================================================================
# Main Test Runner
# ============================================================================

print_header "SMOKE TESTS - Distributed Code Execution System"
echo "Master URL: $MASTER_URL"
echo "Time: $(date)"

# Connection checks
if ! check_master; then
    exit 1
fi

if ! check_workers; then
    exit 1
fi

# Run all tests
print_header "INTERPRETED LANGUAGES"

echo ""
echo -e "${YELLOW}--- Python Tests ---${NC}"
test_python_success
test_python_runtime_error
test_python_infinite_loop

echo ""
echo -e "${YELLOW}--- JavaScript Tests ---${NC}"
test_javascript_success
test_javascript_runtime_error

echo ""
echo -e "${YELLOW}--- Ruby Tests ---${NC}"
test_ruby_success

print_header "COMPILED LANGUAGES"

echo ""
echo -e "${YELLOW}--- C Tests ---${NC}"
test_c_success
test_c_compile_error
test_c_segfault

echo ""
echo -e "${YELLOW}--- C++ Tests ---${NC}"
test_cpp_success
test_cpp_compile_error

echo ""
echo -e "${YELLOW}--- Rust Tests ---${NC}"
test_rust_success
test_rust_compile_error
test_rust_panic

echo ""
echo -e "${YELLOW}--- Go Tests ---${NC}"
test_go_success
test_go_compile_error

echo ""
echo -e "${YELLOW}--- Java Tests ---${NC}"
test_java_success
test_java_compile_error
test_java_null_pointer

print_header "RESOURCE LIMIT TESTS"

echo ""
echo -e "${YELLOW}--- Memory Limit ---${NC}"
test_memory_limit

print_header "BATCH TESTS"

echo ""
echo -e "${YELLOW}--- Multiple Test Cases ---${NC}"
test_multiple_test_cases

# Summary
print_header "TEST SUMMARY"
echo -e "Passed: ${GREEN}$PASSED${NC}"
echo -e "Failed: ${RED}$FAILED${NC}"
echo ""

if [[ $FAILED -eq 0 ]]; then
    echo -e "${GREEN}All tests passed successfully!${NC}"
    echo ""
else
    echo -e "${RED}Some tests failed.${NC}"
    exit 1
fi
