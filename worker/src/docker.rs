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

use crate::config::WorkerConfig;

/// Docker executor for sandboxed code execution
pub struct DockerExecutor {
    docker: Docker,
    config: WorkerConfig,
}

impl DockerExecutor {
    pub fn new(config: WorkerConfig) -> Result<Self, bollard::errors::Error> {
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
        let exec_result = self
            .exec_in_container(&container_name, &compile_cmd, self.config.compile_timeout)
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
        let peak_ram: u64 = 0;
        let mut total_cpu_time: u64 = 0;

        let is_interpreted = matches!(
            language.to_lowercase().as_str(),
            "python" | "python3" | "javascript" | "js" | "node" | "ruby"
        );
        let is_java = matches!(language.to_lowercase().as_str(), "java");

        // Create container logic (same as before)
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
                pids_limit: Some(100), // Increased for shell loop
                readonly_rootfs: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        };

        // Create and start container
        if let Err(e) = self.docker.create_container(Some(CreateContainerOptions{name: container_name.clone(), platform: None}), config).await {
              return BatchExecutionResult { job_id: job_id.to_string(), batch_id: batch_id.to_string(), worker_id: worker_id.to_string(), results: vec![], metrics: None, system_error: format!("Failed to create container: {}", e) };
        }
        if let Err(e) = self.docker.start_container(&container_name, None::<StartContainerOptions<String>>).await {
              let _ = self.cleanup_container(&container_name).await;
              return BatchExecutionResult { job_id: job_id.to_string(), batch_id: batch_id.to_string(), worker_id: worker_id.to_string(), results: vec![], metrics: None, system_error: format!("Failed to start container: {}", e) };
        }

        // Upload executable/source (same as before)
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
                 let _ = self.exec_in_container(&container_name, "mkdir -p /tmp/classes && cd /tmp && tar -xf /tmp/java_bundle.tar && chmod +x /tmp/main", Duration::from_secs(10)).await;
            } else {
                 let tar_data = create_tar_archive_executable("main", bin);
                 let _ = self.docker.upload_to_container(&container_name, Some(UploadToContainerOptions { path: "/tmp", ..Default::default() }), tar_data.into()).await;
                 let _ = self.exec_in_container(&container_name, "chmod +x /tmp/main", Duration::from_secs(5)).await;
            }
        }

        // Prepare inputs
        // We create a tar with a directory 'inputs/' containing input_0, input_1, etc.
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
        let exec_cmd = if is_java { "java -cp /tmp Main" } else if is_interpreted {
            match language.to_lowercase().as_str() {
                "python" | "python3" => "python /tmp/main.py",
                "javascript" | "js" | "node" => "node /tmp/main.js",
                "ruby" => "ruby /tmp/main.rb",
                _ => "/tmp/main",
            }
        } else { "/tmp/main" };

        let timeout_s = (time_limit_ms as f64 / 1000.0).ceil() as u64;
        
        // Shell script to execute all
        // We use 'timeout' command. Busybox timeout is present in alpine. Debian has coreutils.
        // We output delimiters: >>>START:{idx}<<<\n{stdout}\n>>>END:{idx}<<<\n>>>STDERR:{idx}<<<\n{stderr}\n>>>EXIT:{idx}:{code}<<<
        let script = format!(
            r#"
            for i in $(ls /tmp/inputs); do
                echo ">>>START:$i<<<"
                # Run with timeout. Input piped from file.
                timeout {}s {} < /tmp/inputs/$i > /tmp/out.$i 2> /tmp/err.$i
                EXIT_CODE=$?
                cat /tmp/out.$i
                echo ">>>END:$i<<<"
                echo ">>>STDERR:$i<<<"
                cat /tmp/err.$i
                echo ">>>EXIT:$i:$EXIT_CODE<<<"
            done
            "#,
            timeout_s + 1, // Add buffer
            exec_cmd
        );

        // Execute huge batch script
        // We give it plenty of time total: (time_limit * count) + overhead
        let batch_timeout = Duration::from_millis(time_limit_ms as u64 * test_cases.len() as u64 + 5000);
        let exec_res = self.exec_in_container(&container_name, &script, batch_timeout).await;

        match exec_res {
            Ok((_, output)) => {
                 // Parse output
                 // This is simple string parsing.
                 for (i, tc) in test_cases.iter().enumerate() {
                      let start_tag = format!(">>>START:{}<<<", i);
                      let end_tag = format!(">>>END:{}<<<", i);
                      let stderr_tag = format!(">>>STDERR:{}<<<", i);
                      let exit_tag = format!(">>>EXIT:{}:", i);
                      
                      if let (Some(s_idx), Some(e_idx)) = (output.find(&start_tag), output.find(&end_tag)) {
                           let stdout_content = output[s_idx + start_tag.len()..e_idx].trim();
                           
                           // Find stderr
                           let stderr_content = if let Some(se_idx) = output.find(&stderr_tag) {
                                let after_stderr = &output[se_idx + stderr_tag.len()..];
                                if let Some(exit_idx) = after_stderr.find(&format!(">>>EXIT:{}:", i)) {
                                     after_stderr[..exit_idx].trim()
                                } else { "" }
                           } else { "" };
                           
                           // Find exit code
                           let exit_code = if let Some(ext_idx) = output.find(&exit_tag) {
                                let rem = &output[ext_idx + exit_tag.len()..];
                                if let Some(end_marker) = rem.find("<<<") {
                                    rem[..end_marker].parse::<i32>().unwrap_or(-1)
                                } else { -1 }
                           } else { -1 };
                           
                           let expected = tc.expected_output.trim();
                           // Check status
                           // timeout returns 124 or 143 usually
                           let is_tle = exit_code == 124 || exit_code == 143; 
                           // OOM detection is harder here but timeout usually covers it or 137 exit code from shell wrapper if killed?
                           // Actually the wrapper `timeout` kills it.
                           // We will assume 137 or Killed in stderr = MLE/OOM
                           let is_oom = exit_code == 137 || stderr_content.contains("Killed");

                           let status = if is_tle { "TLE" }
                                        else if is_oom { "MLE" }
                                        else if exit_code != 0 { "RE" }
                                        else if stdout_content == expected { "PASSED" }
                                        else { "FAILED" };

                           results.push(TestCaseResult {
                               test_id: tc.id.clone(),
                               status: status.to_string(),
                               stdout: stdout_content.to_string(),
                               stderr: stderr_content.to_string(),
                               time_ms: 0, // Hard to measure individually accurately in batch without sophisticated wrapper
                               memory_bytes: 0,
                           });

                      } else {
                           // Missing execution (maybe script crashed early?)
                           results.push(TestCaseResult { test_id: tc.id.clone(), status: "SKIPPED".to_string(), stdout: "".to_string(), stderr: "Execution skipped".to_string(), time_ms: 0, memory_bytes: 0 });
                      }
                 }
            }
            Err(e) => {
                 // Whole batch failed
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
                peak_ram_bytes: 0, // Not tracked in batch mode yet
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
