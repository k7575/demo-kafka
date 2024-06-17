use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message};

#[tokio::main]
async fn main() {
    tokio::spawn(consumer());
    tokio::spawn(producer());
    tokio::signal::ctrl_c().await.unwrap();
}

async fn producer() {
    let p = ClientConfig::new()
        .set("bootstrap.servers", "192.168.56.3:9092")
        .set("queue.buffering.max.ms", "0")
        .create::<FutureProducer>()
        .expect("Failed to create client");

    loop {
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
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
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
        let message = c.recv().await.unwrap();
        dbg!(message.topic());
        dbg!(message.partition());
        dbg!(message.offset());
        dbg!(message.timestamp());
        dbg!(message.headers().unwrap().get(0));
        let data = message.payload().unwrap().to_vec();
        println!("Consumer read: {}", unsafe {
            String::from_utf8_unchecked(data)
        });
    }
}
