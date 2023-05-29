use std::time::Duration;

use kafka::Error;

use kafka::client::{Compression, KafkaClient, RequiredAcks, DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS};
use kafka::producer::{AsBytes, Producer, Record, DEFAULT_ACK_TIMEOUT_MILLIS};

fn main() -> Result<(), Error> {
    let mut client = KafkaClient::new(vec!["localhost:9092".to_string()]);
    client.set_client_id("kafka-console-producer".into());
    client.load_metadata_all()?;
    client.topics().contains("quickstart-events");

    let mut producer = Producer::from_client(client)
        .with_ack_timeout(Duration::from_secs(10))
        .with_required_acks(RequiredAcks::All)
        .with_compression(Compression::NONE)
        .with_connection_idle_timeout(Duration::from_secs(10))
        // .with_partitioner(partitioner)
        .create()?;

    let mut rec = Record::from_value("quickstart-events", "hello".to_string());
    let res = producer.send(&rec)?;
    println!("send: {:?}", res);

    Ok(())
}
