//! CLI to reproduce SpacetimeDB timestamp collision.
//!
//! Creates 10 connections that each send 100 messages concurrently.
//! Also subscribes to the table to check if subscribers see monotonic timestamps.

mod generated;

use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::mpsc;
use std::sync::Arc;

use anyhow::Result;
use generated::insert_row_reducer::insert_row;
use generated::my_table_table::MyTableTableAccess;
use generated::DbConnection;
use spacetimedb_sdk::{DbContext, Status, Table};

const SERVER: &str = "http://127.0.0.1:3000";
const DATABASE: &str = "timestamp-collision";
const CONNECTION_COUNT: usize = 10;
const MESSAGES_PER_CONNECTION: u32 = 100;
const TOTAL_MESSAGES: u32 = CONNECTION_COUNT as u32 * MESSAGES_PER_CONNECTION;

fn main() -> Result<()> {
    println!(
        "Creating {} connections, each sending {} messages ({} total)...",
        CONNECTION_COUNT, MESSAGES_PER_CONNECTION, TOTAL_MESSAGES
    );

    let success_count = Arc::new(AtomicU32::new(0));
    let error_count = Arc::new(AtomicU32::new(0));
    let sub_error_count = Arc::new(AtomicU32::new(0));
    let (done_tx, done_rx) = mpsc::sync_channel::<()>(1);

    let mut connections = Vec::new();
    let mut connect_rxs = Vec::new();

    // Create all connections
    for i in 0..CONNECTION_COUNT {
        let (connect_tx, connect_rx) = mpsc::sync_channel(1);
        connect_rxs.push(connect_rx);

        let success_count = Arc::clone(&success_count);
        let error_count = Arc::clone(&error_count);
        let sub_error_count_clone = Arc::clone(&sub_error_count);
        let done_tx = done_tx.clone();

        // Track the last timestamp seen by this connection's subscription
        // Using AtomicI64 since Timestamp is i64 microseconds
        let last_sub_ts = Arc::new(AtomicI64::new(i64::MIN));

        let conn = DbConnection::builder()
            .with_uri(SERVER)
            .with_module_name(DATABASE)
            .on_connect(move |ctx, _identity, _token| {
                // Subscribe to my_table
                let last_sub_ts = Arc::clone(&last_sub_ts);
                let sub_error_count = Arc::clone(&sub_error_count_clone);
                let conn_id = i;

                ctx.db.my_table().on_insert(move |_ctx, row| {
                    let new_ts = row.ts.to_micros_since_unix_epoch();
                    let prev_ts = last_sub_ts.swap(new_ts, Ordering::SeqCst);

                    if prev_ts != i64::MIN {
                        if new_ts == prev_ts {
                            let err_num = sub_error_count.fetch_add(1, Ordering::SeqCst) + 1;
                            eprintln!(
                                "SUBSCRIPTION ERROR #{} (conn {}): TIMESTAMP COLLISION! new_ts={} == prev_ts={}",
                                err_num, conn_id, new_ts, prev_ts
                            );
                        } else if new_ts < prev_ts {
                            let err_num = sub_error_count.fetch_add(1, Ordering::SeqCst) + 1;
                            eprintln!(
                                "SUBSCRIPTION ERROR #{} (conn {}): TIMESTAMP WENT BACKWARDS! new_ts={} < prev_ts={}",
                                err_num, conn_id, new_ts, prev_ts
                            );
                        }
                    }
                });

                // Subscribe to the table
                ctx.subscription_builder()
                    .subscribe(["SELECT * FROM my_table"]);

                let _ = connect_tx.send(());
            })
            .on_connect_error(move |_ctx, err| {
                panic!("Connection {} error: {:?}", i, err);
            })
            .build()?;

        // Register reducer callback
        let success_count2 = Arc::clone(&success_count);
        let error_count2 = Arc::clone(&error_count);
        let done_tx2 = done_tx.clone();

        conn.reducers.on_insert_row(move |ctx| {
            match &ctx.event.status {
                Status::Committed => {
                    let count = success_count2.fetch_add(1, Ordering::SeqCst) + 1;
                    if count % 1000 == 0 {
                        println!("Inserted {} rows...", count);
                    }
                    if count + error_count2.load(Ordering::SeqCst) >= TOTAL_MESSAGES {
                        let _ = done_tx2.send(());
                    }
                }
                Status::Failed(err) => {
                    let err_count = error_count2.fetch_add(1, Ordering::SeqCst) + 1;
                    eprintln!("REDUCER ERROR #{}: {}", err_count, err);
                    if success_count2.load(Ordering::SeqCst) + err_count >= TOTAL_MESSAGES {
                        let _ = done_tx2.send(());
                    }
                }
                Status::OutOfEnergy => {
                    panic!("OUT OF ENERGY!");
                }
            }
        });

        // Run connection event loop in background
        conn.run_threaded();
        connections.push(conn);
    }

    // Wait for all connections to be established
    println!("Waiting for {} connections to establish...", CONNECTION_COUNT);
    for (i, rx) in connect_rxs.into_iter().enumerate() {
        rx.recv().expect(&format!("Connection {} should connect", i));
    }
    println!("All connections established!");

    // Send messages from all connections concurrently
    println!("Sending {} messages per connection...", MESSAGES_PER_CONNECTION);
    for conn in &connections {
        for _ in 0..MESSAGES_PER_CONNECTION {
            conn.reducers.insert_row()?;
        }
    }

    // Wait for all callbacks
    println!("Waiting for all callbacks...");
    done_rx.recv().expect("Should complete all inserts");

    // Give a moment for subscription callbacks to finish
    std::thread::sleep(std::time::Duration::from_millis(500));

    let final_success = success_count.load(Ordering::SeqCst);
    let final_errors = error_count.load(Ordering::SeqCst);
    let final_sub_errors = sub_error_count.load(Ordering::SeqCst);

    println!();
    println!("=== RESULTS ===");
    println!(
        "Reducer calls: {} succeeded, {} failed out of {} total",
        final_success, final_errors, TOTAL_MESSAGES
    );
    println!(
        "Subscription non-monotonicity errors: {}",
        final_sub_errors
    );

    if final_errors > 0 || final_sub_errors > 0 {
        std::process::exit(1);
    }

    Ok(())
}
