use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Acquire, SeqCst};
use std::time::Duration;

static EXIT: AtomicBool = AtomicBool::new(false);

#[tokio::main]
async fn main() {
    let t1 = tokio::spawn(producer());
    let t2 = tokio::spawn(consumer());
    tokio::signal::ctrl_c().await.unwrap();
    EXIT.store(true, SeqCst);
    t1.await.unwrap();
    t2.await.unwrap();
}

async fn producer() {
    let p = ClientConfig::new()
        .set("bootstrap.servers", "192.168.56.3:9092")
        .set("queue.buffering.max.ms", "0")
        .create::<FutureProducer>()
        .expect("Failed to create client");

    loop {
        if EXIT.load(Acquire) {
            return;
        }

        let message = &format!("Message: {}", chrono::Local::now().to_string());
        p.send(
            FutureRecord::<(), _>::to("test")
                .payload(message)
                .headers(OwnedHeaders::new().insert(Header {
                    key: "Custom",
                    value: Some("Ok"),
                })),
            Timeout::Never,
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
        println!("Producer write: {}", message);
    }
}

async fn consumer() {
    let c = ClientConfig::new()
        .set("bootstrap.servers", "192.168.56.3:9092")
        .set("enable.partition.eof", "false")
        .set("group.id", "test_group_1")
        .set("client.id", "test_client_1")
        //.set("enable.auto.commit", "false")
        //.set("enable.auto.offset.store", "false")
        .create::<StreamConsumer>()
        .expect("Failed to create client");

    c.subscribe(&["test"]).unwrap();
    loop {
        if EXIT.load(Acquire) {
            return;
        }
        tokio::select! {
            v = c.recv() => {
                if let Ok(message) = v {
                    dbg!(message.topic());
                    dbg!(message.partition());
                    dbg!(message.offset());
                    dbg!(message.timestamp());
                    if let Some(h) = message.headers() {
                        dbg!(h.get(0));
                    }
                    if let Some(data) = message.payload() {
                        println!("Consumer read: {}", unsafe {
                            String::from_utf8_unchecked(data.to_vec())
                        });
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(200)) =>  {}
        }
    }
}
