use std::env;
use std::fs;
use std::io::{Write, Read};
use std::process::{Command, Stdio};
use std::time::Instant;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: runner <cmd> <inputs_dir> [timeout_ms]");
        return;
    }
    let cmd = &args[1];
    let inputs_dir = &args[2];
    let timeout_ms: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1000);
    // Convert ms to seconds for timeout command (ceil)
    let timeout_s = (timeout_ms as f64 / 1000.0).ceil() as u64;

    let entries = fs::read_dir(inputs_dir).unwrap();
    let mut input_files: Vec<_> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();
    // Sort by numeric filename usually "0", "1", etc. but glob sort is safer or assume inputs/0, inputs/1
    // We strictly assume filenames are integers or we sort alphabetically
    input_files.sort();

    print!("[");
    for (i, input_path) in input_files.iter().enumerate() {
        if i > 0 { print!(","); }

        let input_data = fs::read_to_string(&input_path).unwrap_or_default();
        // Extract test ID from filename if possible, else use index
        let test_fn = input_path.file_name().unwrap().to_string_lossy();
        
        let start = Instant::now();
        
        // Use timeout command available in alpine/debian
        // timeout <sec> <cmd>
        // We use sh -c to allow complex commands
        let child = Command::new("timeout")
            .arg(format!("{}s", timeout_s + 1)) // +1s buffer for soft kill
            .arg("sh")
            .arg("-c")
            .arg(cmd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn();

        match child {
            Ok(mut child) => {
                if let Some(mut stdin) = child.stdin.take() {
                     let _ = stdin.write_all(input_data.as_bytes());
                }
                
                let output = child.wait_with_output();
                let duration = start.elapsed().as_millis() as i32;

                match output {
                    Ok(out) => {
                        let exit_code = out.status.code().unwrap_or(-1);
                        let stdout = String::from_utf8_lossy(&out.stdout);
                        let stderr = String::from_utf8_lossy(&out.stderr);
                        
                    

                        // Check for timeout
                        // Check for timeout
                        // Exit code 124 is timeout in GNU coreutils and BusyBox
                        let status = if exit_code == 124 || exit_code == 143 {
                            "TLE"
                        } else if exit_code != 0 {
                            // Check contents for common errors
                            if stderr.contains("Killed") { "MLE" } else { "RE" }
                        } else {
                            "OK" // Caller verifies stdout matches expected
                        };

                        // Sanitize JSON strings
                        fn escape_json_string(s: &str) -> String {
                            let mut escaped = String::with_capacity(s.len() + 2);
                            escaped.push('"');
                            for c in s.chars() {
                                match c {
                                    '"' => escaped.push_str("\\\""),
                                    '\\' => escaped.push_str("\\\\"),
                                    '\x08' => escaped.push_str("\\b"),
                                    '\x0c' => escaped.push_str("\\f"),
                                    '\n' => escaped.push_str("\\n"),
                                    '\r' => escaped.push_str("\\r"),
                                    '\t' => escaped.push_str("\\t"),
                                    c if c.is_control() => {
                                        escaped.push_str(&format!("\\u{:04x}", c as u32));
                                    }
                                    c => escaped.push(c),
                                }
                            }
                            escaped.push('"');
                            escaped
                        }

                        let safe_stdout = escape_json_string(&stdout);
                        let safe_stderr = escape_json_string(&stderr);
                        
                        print!(r##"{{"test_id":"{}", "status":"{}", "exit_code":{}, "stdout":{}, "stderr":{}, "time_ms":{}, "memory_bytes":0}}"##, 
                            test_fn, status, exit_code, safe_stdout, safe_stderr, duration);
                    },
                    Err(_) => {
                         print!(r##"{{"test_id":"{}", "status":"RE", "exit_code":-1, "stdout":"", "stderr":"spawn failed", "time_ms":0, "memory_bytes":0}}"##, test_fn);
                    }
                }
            },
            Err(_) => {
                 print!(r#"{{"test_id":"{}", "status":"RE", "exit_code":-1, "stdout":"", "stderr":"spawn failed", "time_ms":0, "memory_bytes":0}}"#, test_fn);
            }
        }
    }
    print!("]");
}
