use std::thread::{Thread, self};
use std::time::Duration;

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::Error;

fn main() -> Result<(), Error> {
    let mut con = Consumer::from_hosts(vec!["localhost:9092".to_string()])
        .with_topic("quickstart-events".to_string())
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()?;

    loop {
        let mss = con.poll()?;

        if mss.is_empty() {
            println!("no message");
            // return Ok(());
            thread::sleep(Duration::from_secs(3));
        }

        for ms in mss.iter() {
            for m in ms.messages() {
                println!(
                    "{}:{}@{}: {:?}",
                    ms.topic(),
                    ms.partition(),
                    m.offset,
                    String::from_utf8(m.value.to_vec()).unwrap()
                )
            }

            let _ = con.consume_messageset(ms);
        }
        con.commit_consumed()?;
    }

    // Ok(())
}
