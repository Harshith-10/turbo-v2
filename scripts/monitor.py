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
