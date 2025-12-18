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
use std::time::{Duration, Instant};
use tokio::time::timeout;

/// Docker executor for sandboxed code execution
pub struct DockerExecutor {
    docker: Docker,
}

impl DockerExecutor {
    pub fn new() -> Result<Self, bollard::errors::Error> {
        let docker = Docker::connect_with_local_defaults()?;
        Ok(Self { docker })
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
                format!("g++ {} -o /tmp/main /tmp/main.cpp", flags.join(" ")),
            ),
            "c" => (
                "gcc:latest",
                "main.c",
                format!("gcc {} -o /tmp/main /tmp/main.c", flags.join(" ")),
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
                // Compile and create a simple wrapper script
                "javac /tmp/Main.java -d /tmp && echo '#!/bin/sh\njava -cp /tmp Main' > /tmp/main && chmod +x /tmp/main".to_string(),
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
                memory: Some(512 * 1024 * 1024), // 512MB
                nano_cpus: Some(2_000_000_000),  // 2 CPUs
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
        let exec_result = self
            .exec_in_container(&container_name, &compile_cmd, Duration::from_secs(60))
            .await;

        let (success, compiler_output) = match exec_result {
            Ok((exit_code, output)) => (exit_code == 0, output),
            Err(e) => (false, e),
        };

        // Download binary if successful
        let binary_payload = if success {
            self.download_file(&container_name, "/tmp/main")
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

    /// Execute a batch of test cases
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
        let mut peak_ram: u64 = 0;
        let mut total_cpu_time: u64 = 0;

        // Determine execution method based on language
        // Interpreted languages run source directly; compiled languages run binaries
        let is_interpreted = matches!(
            language.to_lowercase().as_str(),
            "python" | "python3" | "javascript" | "js" | "node" | "ruby"
        );
        
        // Java is special - compiled but runs on JVM
        let is_java = matches!(language.to_lowercase().as_str(), "java");

        // Create container
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
            cmd: Some(vec!["sleep".to_string(), "300".to_string()]),
            host_config: Some(bollard::service::HostConfig {
                memory: Some((memory_limit_mb as i64) * 1024 * 1024),
                nano_cpus: Some(1_000_000_000), // 1 CPU
                network_mode: Some("none".to_string()),
                pids_limit: Some(50),
                readonly_rootfs: Some(false), // Need write for /tmp
                ..Default::default()
            }),
            ..Default::default()
        };

        if let Err(e) = self
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
            return BatchExecutionResult {
                job_id: job_id.to_string(),
                batch_id: batch_id.to_string(),
                worker_id: worker_id.to_string(),
                results: vec![],
                metrics: Some(ResourceMetrics {
                    peak_ram_bytes: 0,
                    total_cpu_time_ms: 0,
                }),
                system_error: format!("Failed to create container: {}", e),
            };
        }

        // Start container
        if let Err(e) = self
            .docker
            .start_container(&container_name, None::<StartContainerOptions<String>>)
            .await
        {
            let _ = self.cleanup_container(&container_name).await;
            return BatchExecutionResult {
                job_id: job_id.to_string(),
                batch_id: batch_id.to_string(),
                worker_id: worker_id.to_string(),
                results: vec![],
                metrics: Some(ResourceMetrics {
                    peak_ram_bytes: 0,
                    total_cpu_time_ms: 0,
                }),
                system_error: format!("Failed to start container: {}", e),
            };
        }

        // Upload executable or source
        if is_interpreted {
            if let Some(src) = source_code {
                let (filename, _) = match language.to_lowercase().as_str() {
                    "python" | "python3" => ("main.py", "python /tmp/main.py"),
                    "javascript" | "js" | "node" => ("main.js", "node /tmp/main.js"),
                    "ruby" => ("main.rb", "ruby /tmp/main.rb"),
                    _ => ("main.txt", "cat /tmp/main.txt"),
                };
                let tar_data = create_tar_archive(filename, src.as_bytes());
                let _ = self
                    .docker
                    .upload_to_container(
                        &container_name,
                        Some(UploadToContainerOptions {
                            path: "/tmp",
                            ..Default::default()
                        }),
                        tar_data.into(),
                    )
                    .await;
            }
        } else if let Some(bin) = binary {
            let tar_data = create_tar_archive_executable("main", bin);
            let _ = self
                .docker
                .upload_to_container(
                    &container_name,
                    Some(UploadToContainerOptions {
                        path: "/tmp",
                        ..Default::default()
                    }),
                    tar_data.into(),
                )
                .await;
            // Make executable
            let _ = self
                .exec_in_container(&container_name, "chmod +x /tmp/main", Duration::from_secs(5))
                .await;
        }

        // Execute each test case
        let exec_cmd = if is_java {
            // Java runs the compiled .class file
            "java -cp /tmp Main"
        } else if is_interpreted {
            match language.to_lowercase().as_str() {
                "python" | "python3" => "python /tmp/main.py",
                "javascript" | "js" | "node" => "node /tmp/main.js",
                "ruby" => "ruby /tmp/main.rb",
                _ => "/tmp/main",
            }
        } else {
            "/tmp/main"
        };

        for tc in test_cases {
            let start = Instant::now();

            let result = self
                .run_with_input(
                    &container_name,
                    exec_cmd,
                    &tc.input,
                    Duration::from_millis(time_limit_ms as u64),
                )
                .await;

            let elapsed_ms = start.elapsed().as_millis() as i32;
            total_cpu_time += elapsed_ms as u64;

            let tc_result = match result {
                Ok((exit_code, stdout, stderr)) => {
                    let actual_output = stdout.trim();
                    let expected_output = tc.expected_output.trim();

                    let status = if exit_code != 0 {
                        "RE" // Runtime Error
                    } else if actual_output == expected_output {
                        "PASSED"
                    } else {
                        "FAILED"
                    };

                    TestCaseResult {
                        test_id: tc.id.clone(),
                        status: status.to_string(),
                        stdout,
                        stderr,
                        time_ms: elapsed_ms,
                        memory_bytes: 0, // TODO: get actual memory usage
                    }
                }
                Err(e) => {
                    let status = if e.contains("timeout") { "TLE" } else { "RE" };
                    TestCaseResult {
                        test_id: tc.id.clone(),
                        status: status.to_string(),
                        stdout: String::new(),
                        stderr: e,
                        time_ms: elapsed_ms,
                        memory_bytes: 0,
                    }
                }
            };

            results.push(tc_result);
        }

        // Cleanup
        let _ = self.cleanup_container(&container_name).await;

        BatchExecutionResult {
            job_id: job_id.to_string(),
            batch_id: batch_id.to_string(),
            worker_id: worker_id.to_string(),
            results,
            metrics: Some(ResourceMetrics {
                peak_ram_bytes: peak_ram,
                total_cpu_time_ms: total_cpu_time,
            }),
            system_error: String::new(),
        }
    }

    /// Execute a command in a container with timeout
    async fn exec_in_container(
        &self,
        container: &str,
        cmd: &str,
        timeout_duration: Duration,
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
                    while let Some(chunk) = output.next().await {
                        if let Ok(msg) = chunk {
                            result.push_str(&msg.to_string());
                        }
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
            .exec_in_container(container, &full_cmd, timeout_duration)
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
