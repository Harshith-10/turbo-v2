use clap::Parser;
use colored::*;
use eventsource_stream::Eventsource;
use futures::stream::StreamExt;
use hdrhistogram::Histogram;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Master node URL
    #[arg(short, long, default_value = "http://127.0.0.1:8080")]
    url: String,

    /// Number of concurrent users
    #[arg(short, long, default_value_t = 300)]
    users: usize,

    /// Number of jobs per user
    #[arg(short, long, default_value_t = 10)]
    jobs: usize,

    /// Delay between jobs in ms
    #[arg(short, long, default_value_t = 60000)]
    delay: u64,

    /// Number of test cases per job
    #[arg(short, long, default_value_t = 50)]
    test_cases: usize,
}

#[derive(Serialize, Deserialize, Debug)]
struct SubmitRequest {
    language: String,
    source_code: String,
    test_cases: Vec<TestCaseInput>,
}

#[derive(Serialize, Deserialize, Debug)]
struct TestCaseInput {
    id: String,
    input: String,
    expected_output: String,
}

#[derive(Deserialize, Debug)]
struct SubmitResponse {
    job_id: String,
}

#[derive(Debug)]
struct JobMetrics {
    job_id: String,
    submission_time: Duration,
    turnaround_time: Duration,
    success: bool,
}

#[derive(Clone)]
struct MetricsCollector {
    submission_times: Arc<Mutex<Histogram<u64>>>,
    turnaround_times: Arc<Mutex<Histogram<u64>>>,
    total_jobs: Arc<Mutex<usize>>,
    failed_jobs: Arc<Mutex<usize>>,
}

impl MetricsCollector {
    fn new() -> Self {
        Self {
            submission_times: Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap())),
            turnaround_times: Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap())),
            total_jobs: Arc::new(Mutex::new(0)),
            failed_jobs: Arc::new(Mutex::new(0)),
        }
    }

    fn record(&self, metrics: JobMetrics) {
        let mut sub = self.submission_times.lock().unwrap();
        sub.record(metrics.submission_time.as_millis() as u64).unwrap();

        let mut turn = self.turnaround_times.lock().unwrap();
        turn.record(metrics.turnaround_time.as_millis() as u64).unwrap();

        let mut total = self.total_jobs.lock().unwrap();
        *total += 1;

        if !metrics.success {
            let mut failed = self.failed_jobs.lock().unwrap();
            *failed += 1;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("{}", "Starting Load Test...".green().bold());
    println!("Target: {}", args.url);
    println!("Users: {}", args.users);
    println!("Jobs/User: {}", args.jobs);

    let client = Client::new();
    let metrics = MetricsCollector::new();
    let multi_pb = MultiProgress::new();
    let main_pb = multi_pb.add(ProgressBar::new((args.users * args.jobs) as u64));
    main_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

    let mut handles = vec![];

    for user_id in 0..args.users {
        let client = client.clone();
        let url = args.url.clone();
        let jobs = args.jobs;
        let delay = args.delay;
        let metrics = metrics.clone();
        let pb = main_pb.clone();

        let num_test_cases = args.test_cases;

        let handle = tokio::spawn(async move {
            for i in 0..jobs {
                if let Err(e) = run_job(user_id, i, &client, &url, &metrics, num_test_cases).await {
                    pb.println(format!("User {} Job {} failed: {}", user_id, i, e));
                }
                pb.inc(1);
                sleep(Duration::from_millis(delay)).await;
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await?;
    }

    main_pb.finish_with_message("Done");

    // Report
    println!("\n{}", "Load Test Results".bold().underline());
    
    let total = *metrics.total_jobs.lock().unwrap();
    let failed = *metrics.failed_jobs.lock().unwrap();
    let sub = metrics.submission_times.lock().unwrap();
    let turn = metrics.turnaround_times.lock().unwrap();

    println!("Total Jobs: {}", total);
    println!("Failed Jobs: {} ({:.2}%)", failed, (failed as f64 / total as f64) * 100.0);
    
    println!("\n{}:", "Submission Latency".cyan());
    println!("  Min: {} ms", sub.min());
    println!("  P50: {} ms", sub.value_at_quantile(0.5));
    println!("  P95: {} ms", sub.value_at_quantile(0.95));
    println!("  P99: {} ms", sub.value_at_quantile(0.99));
    println!("  Max: {} ms", sub.max());

    println!("\n{}:", "Turnaround Time (End-to-End)".cyan());
    println!("  Min: {} ms", turn.min());
    println!("  P50: {} ms", turn.value_at_quantile(0.5));
    println!("  P95: {} ms", turn.value_at_quantile(0.95));
    println!("  P99: {} ms", turn.value_at_quantile(0.99));
    println!("  Max: {} ms", turn.max());

    // Generate JSON report
    let report = serde_json::json!({
        "config": {
            "users": args.users,
            "jobs_per_user": args.jobs,
            "url": args.url
        },
        "results": {
            "total_jobs": total,
            "failed_jobs": failed,
            "submission_latency": {
                "p50": sub.value_at_quantile(0.5),
                "p95": sub.value_at_quantile(0.95),
                "p99": sub.value_at_quantile(0.99)
            },
            "turnaround_time": {
                "p50": turn.value_at_quantile(0.5),
                "p95": turn.value_at_quantile(0.95),
                "p99": turn.value_at_quantile(0.99)
            }
        }
    });

    std::fs::write("load_test_report.json", serde_json::to_string_pretty(&report)?)?;
    println!("\nReport saved to load_test_report.json");

    Ok(())
}

async fn run_job(
    _user_id: usize,
    _job_seq: usize,
    client: &Client,
    base_url: &str,
    metrics: &MetricsCollector,
    num_test_cases: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut test_cases = Vec::with_capacity(num_test_cases);
    for i in 0..num_test_cases {
        test_cases.push(TestCaseInput {
            id: format!("{}", i),
            input: format!("input_{}", i),
            expected_output: format!("input_{}", i), // Echo program for simplicity
        });
    }

    let source_code = r#"
import sys

def main():
    # Echo stdin to stdout
    print(sys.stdin.read().strip())

if __name__ == "__main__":
    main()
"#.to_string();

    let req = SubmitRequest {
        language: "python".to_string(),
        source_code,
        test_cases,
    };

    let start = Instant::now();
    let res = client.post(format!("{}/submit", base_url))
        .json(&req)
        .send()
        .await?;
    
    let submission_time = start.elapsed();
    
    if !res.status().is_success() {
        return Err(format!("Submission failed: {}", res.status()).into());
    }

    let body: SubmitResponse = res.json().await?;
    let job_id = body.job_id;

    // Connect to SSE
    let mut event_stream = client.get(format!("{}/status/{}", base_url, job_id))
        .send()
        .await?
        .bytes_stream()
        .eventsource();

    let mut success = false;
    let mut completed = false;

    while let Some(event) = event_stream.next().await {
        match event {
            Ok(e) => {
                // Parse event data
                if e.data == "stream closed" {
                     break;
                }
                
                // Simple check for completion
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&e.data) {
                    if let Some(type_) = json.get("type") {
                        if type_ == "Completed" {
                            success = true;
                            completed = true;
                            break;
                        } else if type_ == "Error" {
                            success = false;
                            completed = true;
                            break;
                        }
                    }
                }
            }
            Err(_) => {
                // Stream error
                break;
            }
        }
    }

    let turnaround_time = start.elapsed();

    if !completed {
        // Timeout or stream cut
        success = false;
    }

    metrics.record(JobMetrics {
        job_id,
        submission_time,
        turnaround_time,
        success,
    });

    Ok(())
}
