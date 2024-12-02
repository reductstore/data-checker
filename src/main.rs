use futures_util::stream::StreamExt;
use log::{debug, error, info};
use rand::distributions::{Alphanumeric, DistString};
use rand::Rng;
use reduct_base::internal_server_error;
use reduct_rs::{Bucket, ReductClient, ReductError};
use simple_logger::SimpleLogger;
use std::env;
use std::time::{Duration, SystemTime};
use tokio::pin;
use tokio::time::{sleep, Instant};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let server_url = env::var("REDUCT_STORAGE_URL").expect("SERVER_URL must be set");
    let api_token = env::var("REDUCT_API_TOKEN").expect("API_TOKEN must be set");
    let entry_name = env::var("REDUCT_ENTRY_NAME").unwrap_or("test-entry".to_string());
    let role = env::var("REDUCT_ROLE").unwrap_or("reader".to_string());
    let interval_ms = env::var("INTERVAL_MS")
        .unwrap_or("1000".to_string())
        .parse::<u64>()
        .expect("INTERVAL_MS must be a number");
    let interval_ms = Duration::from_millis(interval_ms);

    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();
    info!("Connecting to: {}", server_url);

    let client = ReductClient::builder()
        .url(&server_url)
        .api_token(&api_token)
        .build();

    let run = async move {
        let bucket = client.get_bucket("stress_test").await?;
        if role == "reader" {
            reader(bucket, entry_name, interval_ms).await?;
        } else if role == "writer" {
            writer(bucket, entry_name, interval_ms).await?;
        } else {
            panic!("Invalid role: {}", role);
        }

        Ok::<(), ReductError>(())
    };

    if let Err(e) = run.await {
        error!("{}", e);
    }
}

async fn reader(
    bucket: Bucket,
    entry_name: String,
    interval_ms: Duration,
) -> Result<(), ReductError> {
    loop {
        let now = Instant::now();
        let entry = bucket
            .entries()
            .await?
            .iter()
            .find(|entry| entry.name == entry_name)
            .expect("Entry not found")
            .clone();

        let stream = bucket
            .query(&entry.name)
            .start(SystemTime::now() - interval_ms * 4)
            .stop(SystemTime::now())
            .limit(1)
            .send()
            .await?;

        pin!(stream);
        while let Some(result) = stream.next().await {
            let record = result?;
            let labels = record.labels().clone();
            let data = record.bytes().await?;
            let md5 = format!("{:x}", md5::compute(&data));
            debug!(
                "Read record: size={}, md5={}, labels={:?}",
                data.len(),
                md5,
                labels
            );
            if &md5 != labels.get("md5").unwrap() {
                return Err(internal_server_error!(
                    "MD5 mismatch: expected {}, got {}",
                    labels.get("md5").unwrap(),
                    md5
                ));
            }
        }

        if now.elapsed() < interval_ms {
            sleep(interval_ms - now.elapsed()).await;
        }
    }
}

async fn writer(
    bucket: Bucket,
    entry_name: String,
    interval_ms: Duration,
) -> Result<(), ReductError> {
    let blob = Alphanumeric.sample_string(&mut rand::thread_rng(), 18_000_000);

    loop {
        let now = Instant::now();

        let slice = blob[..rand::thread_rng().gen_range(0..blob.len())].to_string();
        let size = if slice.len() > (blob.len() as f64 * 0.66) as usize {
            "big"
        } else if slice.len() < (blob.len() as f64 * 0.33) as usize {
            "small"
        } else {
            "medium"
        };

        bucket
            .write_record(&entry_name)
            .add_label("md5", &format!("{:x}", md5::compute(&slice)))
            .add_label("size", size)
            .data(slice)
            .send()
            .await?;

        if now.elapsed() < interval_ms {
            sleep(interval_ms - now.elapsed()).await;
        }
    }
}
