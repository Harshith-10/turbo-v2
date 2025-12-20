//! Worker Node - Docker Execution
//!
//! Uses bollard to interact with Docker for sandboxed code execution.

use bollard::container::{
    Config, CreateContainerOptions, RemoveContainerOptions, StartContainerOptions,
    UploadToContainerOptions,
};
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::Docker;
use common::scheduler::{
    BatchExecutionResult, CompileResult, ResourceMetrics, TestCase, TestCaseResult,
};
use futures::StreamExt;
use serde::Deserialize;
use std::time::{Duration, Instant};
use tokio::time::timeout;

use crate::config::WorkerConfig;

const RUNNER_SOURCE: &str = include_str!("runner_code.rs");

/// Docker executor for sandboxed code execution
pub struct DockerExecutor {
    docker: Docker,
    config: WorkerConfig,
}

impl DockerExecutor {
    pub fn new(config: WorkerConfig) -> Result<Self, bollard::errors::Error> {
        // Ensure runner is built
        let runner_path = std::path::Path::new("/tmp/turbo_runner");
        if !runner_path.exists() {
             tracing::info!("Compiling turbo_runner...");
             std::fs::write("/tmp/runner.rs", RUNNER_SOURCE).expect("Failed to write runner source");
             let status = std::process::Command::new("rustc")
                 .args(&["-C", "target-feature=+crt-static", "-O", "/tmp/runner.rs", "-o", "/tmp/turbo_runner"])
                 .status()
                 .expect("Failed to execute rustc");
             if !status.success() {
                 tracing::error!("Failed to compile runner");
             } else {
                 tracing::info!("turbo_runner compiled successfully");
             }
        }

        let docker = Docker::connect_with_local_defaults()?;
        Ok(Self { docker, config })
    }

    /// Compile source code and return the binary
    pub async fn compile(
        &self,
        job_id: &str,
        language: &str,
        source_code: &str,
        flags: &[String],
    ) -> CompileResult {
        let start = Instant::now();

        // Select image and compile command based on language
        // For Java, we compile to bytecode and package it
        let (image, src_file, compile_cmd) = match language.to_lowercase().as_str() {
            "cpp" | "c++" => (
                "gcc:latest",
                "main.cpp",
                format!("g++ -static {} -o /tmp/main /tmp/main.cpp", flags.join(" ")),
            ),
            "c" => (
                "gcc:latest",
                "main.c",
                format!("gcc -static {} -o /tmp/main /tmp/main.c", flags.join(" ")),
            ),
            "rust" => (
                "rust:latest",
                "main.rs",
                format!("rustc {} -o /tmp/main /tmp/main.rs", flags.join(" ")),
            ),
            "go" | "golang" => (
                "golang:latest",
                "main.go",
                "go build -o /tmp/main /tmp/main.go".to_string(),
            ),
            "java" => (
                "eclipse-temurin:25",
                "Main.java",
                // Compile to /tmp/classes, then create tarball with classes and wrapper script
                "mkdir -p /tmp/classes && javac /tmp/Main.java -d /tmp/classes && \
                 cd /tmp && tar -cf /tmp/java_bundle.tar -C /tmp/classes . && \
                 echo '#!/bin/sh\njava -cp /tmp/classes Main' > /tmp/main && chmod +x /tmp/main && \
                 tar -rf /tmp/java_bundle.tar -C /tmp main".to_string(),
            ),
            _ => {
                return CompileResult {
                    job_id: job_id.to_string(),
                    success: false,
                    compiler_output: format!("Unsupported compiled language: {}. Interpreted languages (python, javascript, ruby) don't need compilation.", language),
                    binary_payload: vec![],
                    duration_ms: 0,
                };
            }
        };

        // Create container
        let container_name = format!("compile_{}", job_id.replace('-', "_"));
        let config = Config {
            image: Some(image.to_string()),
            cmd: Some(vec!["sleep".to_string(), "300".to_string()]),
            host_config: Some(bollard::service::HostConfig {
                memory: Some(self.config.compile_memory_limit_bytes),
                nano_cpus: Some(self.config.compile_cpu_nano),
                network_mode: Some("none".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        match self
            .docker
            .create_container(
                Some(CreateContainerOptions {
                    name: container_name.clone(),
                    platform: None,
                }),
                config,
            )
            .await
        {
            Ok(_) => {}
            Err(e) => {
                return CompileResult {
                    job_id: job_id.to_string(),
                    success: false,
                    compiler_output: format!("Failed to create container: {}", e),
                    binary_payload: vec![],
                    duration_ms: start.elapsed().as_millis() as i32,
                };
            }
        }

        // Start container
        if let Err(e) = self
            .docker
            .start_container(&container_name, None::<StartContainerOptions<String>>)
            .await
        {
            let _ = self.cleanup_container(&container_name).await;
            return CompileResult {
                job_id: job_id.to_string(),
                success: false,
                compiler_output: format!("Failed to start container: {}", e),
                binary_payload: vec![],
                duration_ms: start.elapsed().as_millis() as i32,
            };
        }

        // Upload source code
        let tar_data = create_tar_archive(src_file, source_code.as_bytes());
        if let Err(e) = self
            .docker
            .upload_to_container(
                &container_name,
                Some(UploadToContainerOptions {
                    path: "/tmp",
                    ..Default::default()
                }),
                tar_data.into(),
            )
            .await
        {
            let _ = self.cleanup_container(&container_name).await;
            return CompileResult {
                job_id: job_id.to_string(),
                success: false,
                compiler_output: format!("Failed to upload source: {}", e),
                binary_payload: vec![],
                duration_ms: start.elapsed().as_millis() as i32,
            };
        }

        // Execute compile command
        // Limit compile output to 1MB
        let exec_result = self
            .exec_in_container(&container_name, &compile_cmd, self.config.compile_timeout, 1024 * 1024)
            .await;

        let (success, compiler_output) = match exec_result {
            Ok((exit_code, output)) => (exit_code == 0, output),
            Err(e) => (false, e),
        };

        // Download binary if successful
        // For Java, download the bundle tarball instead of just the wrapper
        let binary_payload = if success {
            let download_path = if language.to_lowercase() == "java" {
                "/tmp/java_bundle.tar"
            } else {
                "/tmp/main"
            };
            self.download_file(&container_name, download_path)
                .await
                .unwrap_or_default()
        } else {
            vec![]
        };

        // Cleanup
        let _ = self.cleanup_container(&container_name).await;

        CompileResult {
            job_id: job_id.to_string(),
            success,
            compiler_output,
            binary_payload,
            duration_ms: start.elapsed().as_millis() as i32,
        }
    }

    /// Execute a batch of test cases optimized
    pub async fn execute_batch(
        &self,
        job_id: &str,
        batch_id: &str,
        worker_id: &str,
        language: &str,
        binary: Option<&[u8]>,
        source_code: Option<&str>,
        test_cases: &[TestCase],
        time_limit_ms: u32,
        memory_limit_mb: u32,
    ) -> BatchExecutionResult {
        let mut results = Vec::new();
        let total_cpu_time: u64 = 0;

        let is_interpreted = matches!(
            language.to_lowercase().as_str(),
            "python" | "python3" | "javascript" | "js" | "node" | "ruby"
        );
        let is_java = matches!(language.to_lowercase().as_str(), "java");

        // Create container logic
        let container_name = format!("run_{}_{}", job_id.replace('-', "_"), batch_id);
        let image = if is_java {
            "eclipse-temurin:25"
        } else if is_interpreted {
            match language.to_lowercase().as_str() {
                "python" | "python3" => "python:3-slim",
                "javascript" | "js" | "node" => "node:slim",
                "ruby" => "ruby:slim",
                _ => "alpine:latest",
            }
        } else {
            "debian:bookworm-slim"
        };

        let config = Config {
            image: Some(image.to_string()),
            cmd: Some(vec!["sleep".to_string(), "3600".to_string()]), // Long sleep to keep alive
            host_config: Some(bollard::service::HostConfig {
                memory: Some((memory_limit_mb as i64) * 1024 * 1024),
                nano_cpus: Some(self.config.execute_cpu_nano),
                network_mode: Some("none".to_string()),
                pids_limit: Some(100),
                readonly_rootfs: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        };

        if let Err(e) = self.docker.create_container(Some(CreateContainerOptions{name: container_name.clone(), platform: None}), config).await {
              return BatchExecutionResult { job_id: job_id.to_string(), batch_id: batch_id.to_string(), worker_id: worker_id.to_string(), results: vec![], metrics: None, system_error: format!("Failed to create container: {}", e) };
        }
        if let Err(e) = self.docker.start_container(&container_name, None::<StartContainerOptions<String>>).await {
              let _ = self.cleanup_container(&container_name).await;
              return BatchExecutionResult { job_id: job_id.to_string(), batch_id: batch_id.to_string(), worker_id: worker_id.to_string(), results: vec![], metrics: None, system_error: format!("Failed to start container: {}", e) };
        }

        // Upload executable/source
        if is_interpreted {
            if let Some(src) = source_code {
                let (filename, _) = match language.to_lowercase().as_str() {
                    "python" | "python3" => ("main.py", ""),
                    "javascript" | "js" | "node" => ("main.js", ""),
                    "ruby" => ("main.rb", ""),
                    _ => ("main.txt", ""),
                };
                let tar_data = create_tar_archive(filename, src.as_bytes());
                let _ = self.docker.upload_to_container(&container_name, Some(UploadToContainerOptions { path: "/tmp", ..Default::default() }), tar_data.into()).await;
            }
        } else if let Some(bin) = binary {
            if is_java {
                 let _ = self.docker.upload_to_container(&container_name, Some(UploadToContainerOptions { path: "/tmp", ..Default::default() }), bin.to_vec().into()).await;
                 let _ = self.exec_in_container(&container_name, "mkdir -p /tmp/classes && cd /tmp && tar -xf /tmp/java_bundle.tar && chmod +x /tmp/main", Duration::from_secs(10), 1024 * 1024).await;
            } else {
                 let tar_data = create_tar_archive_executable("main", bin);
                 let _ = self.docker.upload_to_container(&container_name, Some(UploadToContainerOptions { path: "/tmp", ..Default::default() }), tar_data.into()).await;
                 let _ = self.exec_in_container(&container_name, "chmod +x /tmp/main", Duration::from_secs(5), 1024 * 1024).await;
            }
        }

        // Upload Runner
        if let Ok(runner_bin) = std::fs::read("/tmp/turbo_runner") {
             let _ = self.docker.upload_to_container(&container_name, Some(UploadToContainerOptions { path: "/tmp", ..Default::default() }), create_tar_archive_executable("runner", &runner_bin).into()).await;
             let _ = self.exec_in_container(&container_name, "chmod +x /tmp/runner", Duration::from_secs(5), 1024 * 1024).await;
        } else {
             let _ = self.cleanup_container(&container_name).await;
             return BatchExecutionResult { job_id: job_id.to_string(), batch_id: batch_id.to_string(), worker_id: worker_id.to_string(), results: vec![], metrics: None, system_error: "Runner binary missing on host".to_string() };
        }

        // Prepare inputs
        let mut inputs_archive = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut inputs_archive);
            for (i, tc) in test_cases.iter().enumerate() {
                let mut header = tar::Header::new_gnu();
                header.set_path(format!("inputs/{}", i)).unwrap();
                header.set_size(tc.input.len() as u64);
                header.set_mode(0o644);
                header.set_cksum();
                builder.append(&header, tc.input.as_bytes()).unwrap();
            }
            builder.finish().unwrap();
        }
        let _ = self.docker.upload_to_container(&container_name, Some(UploadToContainerOptions { path: "/tmp", ..Default::default() }), inputs_archive.into()).await;

        // Construct command to run all tests
        let target_cmd = if is_java { "java -cp /tmp Main" } else if is_interpreted {
            match language.to_lowercase().as_str() {
                "python" | "python3" => "python /tmp/main.py",
                "javascript" | "js" | "node" => "node /tmp/main.js",
                "ruby" => "ruby /tmp/main.rb",
                _ => "/tmp/main",
            }
        } else { "/tmp/main" };

        let runner_cmd = format!("/tmp/runner '{}' /tmp/inputs {}", target_cmd, time_limit_ms);
        let batch_timeout = Duration::from_millis(time_limit_ms as u64 * test_cases.len() as u64 + 10000);
        let exec_res = self.exec_in_container(&container_name, &runner_cmd, batch_timeout, 10 * 1024 * 1024).await;

        match exec_res {
            Ok((_, output)) => {
                 tracing::info!("RAW RUNNER OUTPUT: {}", output);
                 let json_start = output.find('[');
                 let json_end = output.rfind(']');
                 
                 if let (Some(s), Some(e)) = (json_start, json_end) {
                      let json_str = &output[s..=e];
                      #[derive(Deserialize)]
                      struct RunnerResult {
                          test_id: String,
                          status: String,
                          stdout: String,
                          stderr: String,
                          time_ms: i32,
                          memory_bytes: i32,
                      }
                      
                      match serde_json::from_str::<Vec<RunnerResult>>(json_str) {
                          Ok(parsed_results) => {
                               for r in parsed_results {
                                   let mut final_result = TestCaseResult {
                                       test_id: r.test_id.clone(),
                                       status: r.status,
                                       stdout: r.stdout,
                                       stderr: r.stderr,
                                       time_ms: r.time_ms,
                                       memory_bytes: r.memory_bytes,
                                   };
                                   
                                   let idx = r.test_id.parse::<usize>().unwrap_or(0);
                                   if let Some(tc) = test_cases.get(idx) {
                                       final_result.test_id = tc.id.clone();
                                       
                                       if final_result.status == "OK" {
                                            if final_result.stdout.trim() == tc.expected_output.trim() {
                                                final_result.status = "PASSED".to_string();
                                            } else {
                                                final_result.status = "FAILED".to_string();
                                            }
                                       }
                                   }
                                   results.push(final_result);
                               }
                          }
                          Err(e) => {
                               tracing::error!("Failed to parse runner execution JSON: {}. Output: {}", e, json_str);
                          }
                      }
                 } else {
                      tracing::error!("No JSON output found from runner. Output: {}", output);
                      return BatchExecutionResult { 
                          job_id: job_id.to_string(), 
                          batch_id: batch_id.to_string(), 
                          worker_id: worker_id.to_string(), 
                          results: vec![], 
                          metrics: None, 
                          system_error: format!("Runner failed to produce output. Raw: {}", output) 
                      };
                 }
            }
            Err(e) => {
                  return BatchExecutionResult { job_id: job_id.to_string(), batch_id: batch_id.to_string(), worker_id: worker_id.to_string(), results: vec![], metrics: None, system_error: format!("Batch execution failed: {}", e) };
            }
        }

        let _ = self.cleanup_container(&container_name).await;

        BatchExecutionResult {
            job_id: job_id.to_string(),
            batch_id: batch_id.to_string(),
            worker_id: worker_id.to_string(),
            results,
            metrics: Some(ResourceMetrics {
                peak_ram_bytes: 0, 
                total_cpu_time_ms: total_cpu_time,
            }),
            system_error: String::new(),
        }
    }

    /// Execute a command in a container with timeout and output limit
    async fn exec_in_container(
        &self,
        container: &str,
        cmd: &str,
        timeout_duration: Duration,
        max_output_bytes: usize,
    ) -> Result<(i64, String), String> {
        let exec = self
            .docker
            .create_exec(
                container,
                CreateExecOptions {
                    cmd: Some(vec!["sh", "-c", cmd]),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| format!("Failed to create exec: {}", e))?;

        let output = timeout(timeout_duration, async {
            match self.docker.start_exec(&exec.id, None).await {
                Ok(StartExecResults::Attached { mut output, .. }) => {
                    let mut result = String::new();
                    let mut truncated = false;
                    while let Some(chunk) = output.next().await {
                        if let Ok(msg) = chunk {
                            let s = msg.to_string();
                            if result.len() + s.len() > max_output_bytes {
                                let remaining = max_output_bytes.saturating_sub(result.len());
                                result.push_str(&s[..remaining]);
                                truncated = true;
                                break; 
                            }
                            result.push_str(&s);
                        }
                    }

                    if truncated {
                       result.push_str("\n[Output Truncated due to size limit]");
                    }

                    // Get exit code
                    let inspect = self.docker.inspect_exec(&exec.id).await.ok();
                    let exit_code = inspect.and_then(|i| i.exit_code).unwrap_or(-1);

                    Ok((exit_code, result))
                }
                Ok(StartExecResults::Detached) => Ok((0, String::new())),
                Err(e) => Err(format!("Exec failed: {}", e)),
            }
        })
        .await
        .map_err(|_| "Execution timeout".to_string())??;

        Ok(output)
    }

    /// Run a command with stdin input
    #[allow(dead_code)]
    async fn run_with_input(
        &self,
        container: &str,
        cmd: &str,
        input: &str,
        timeout_duration: Duration,
    ) -> Result<(i64, String, String), String> {
        // Write input to a file and pipe it
        let input_escaped = input.replace("'", "'\\''");
        let full_cmd = format!("echo '{}' | {}", input_escaped, cmd);

        let (exit_code, output) = self
            .exec_in_container(container, &full_cmd, timeout_duration, 1024 * 1024)
            .await?;

        // Try to split stdout/stderr (simplified - real implementation would capture separately)
        Ok((exit_code, output, String::new()))
    }

    /// Download a file from container
    async fn download_file(&self, container: &str, path: &str) -> Result<Vec<u8>, String> {
        let stream = self
            .docker
            .download_from_container(container, Some(bollard::container::DownloadFromContainerOptions { path }))
            .map(|chunk| chunk.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));

        let bytes: Vec<u8> = tokio_stream::StreamExt::collect::<Vec<_>>(stream)
            .await
            .into_iter()
            .filter_map(|r| r.ok())
            .flatten()
            .collect();

        // Extract from tar
        extract_from_tar(&bytes).ok_or_else(|| "Failed to extract file from tar".to_string())
    }

    /// Remove a container
    async fn cleanup_container(&self, name: &str) -> Result<(), String> {
        self.docker
            .remove_container(
                name,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await
            .map_err(|e| format!("Failed to remove container: {}", e))
    }
}

/// Create a tar archive containing a single file
fn create_tar_archive(filename: &str, content: &[u8]) -> Vec<u8> {

    let mut header = tar::Header::new_gnu();
    header.set_path(filename).unwrap();
    header.set_size(content.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();

    let mut archive = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut archive);
        builder.append(&header, content).unwrap();
        builder.finish().unwrap();
    }
    archive
}

/// Create a tar archive with an executable file
fn create_tar_archive_executable(filename: &str, content: &[u8]) -> Vec<u8> {

    let mut header = tar::Header::new_gnu();
    header.set_path(filename).unwrap();
    header.set_size(content.len() as u64);
    header.set_mode(0o755);
    header.set_cksum();

    let mut archive = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut archive);
        builder.append(&header, content).unwrap();
        builder.finish().unwrap();
    }
    archive
}

/// Extract the first file from a tar archive
fn extract_from_tar(data: &[u8]) -> Option<Vec<u8>> {
    use std::io::Read;

    let mut archive = tar::Archive::new(data);
    for entry in archive.entries().ok()? {
        if let Ok(mut entry) = entry {
            let mut content = Vec::new();
            entry.read_to_end(&mut content).ok()?;
            return Some(content);
        }
    }
    None
}
