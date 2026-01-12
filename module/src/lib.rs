//! Workaround for SpacetimeDB timestamp collision.
//!
//! This module demonstrates a workaround for the ctx.timestamp collision issue
//! by tracking the last used timestamp and ensuring strict monotonicity.

use spacetimedb::{reducer, table, ReducerContext, Table, Timestamp};

#[table(name = my_table, public)]
pub struct MyTable {
    #[primary_key]
    pub ts: Timestamp,
}

/// Singleton table to track the last used timestamp.
/// This allows us to ensure strictly monotonic timestamps even when
/// ctx.timestamp goes backwards or collides.
#[table(name = last_timestamp, public)]
pub struct LastTimestamp {
    #[primary_key]
    pub id: u32, // Always 0, singleton pattern
    pub ts: Timestamp,
}

/// Get a strictly monotonic timestamp.
/// Returns max(ctx.timestamp, last_used_timestamp + 1µs).
fn get_monotonic_timestamp(ctx: &ReducerContext) -> Timestamp {
    // We'd prefer to use Timestamp::now() here since ctx.timestamp is assigned
    // when the reducer is queued, not when it executes. However, Timestamp::now()
    // is not available in WASM modules (it's stubbed and will panic).
    let now = ctx.timestamp;

    // Get the last used timestamp (if any)
    let last_ts = ctx.db.last_timestamp().id().find(0).map(|row| row.ts);

    let new_ts = match last_ts {
        Some(last) => {
            let last_micros = last.to_micros_since_unix_epoch();
            let now_micros = now.to_micros_since_unix_epoch();
            // Use whichever is larger: now, or last + 1µs
            if now_micros > last_micros {
                now
            } else {
                Timestamp::from_micros_since_unix_epoch(last_micros + 1)
            }
        }
        None => now,
    };

    // Update the last timestamp tracker
    if last_ts.is_some() {
        ctx.db.last_timestamp().id().update(LastTimestamp { id: 0, ts: new_ts });
    } else {
        ctx.db.last_timestamp().insert(LastTimestamp { id: 0, ts: new_ts });
    }

    new_ts
}

#[reducer]
pub fn insert_row(ctx: &ReducerContext) -> Result<(), String> {
    let new_ts = get_monotonic_timestamp(ctx);
    let new_ts_micros = new_ts.to_micros_since_unix_epoch();

    // Check against the most recent entry (should never fail now)
    if let Some(last) = ctx.db.my_table().iter().last() {
        let last_ts_micros = last.ts.to_micros_since_unix_epoch();

        if last_ts_micros == new_ts_micros {
            return Err(format!(
                "TIMESTAMP COLLISION! new_ts={:?} ({} micros), existing_ts={:?} ({} micros)",
                new_ts,
                new_ts_micros,
                last.ts,
                last_ts_micros,
            ));
        }

        if last_ts_micros > new_ts_micros {
            return Err(format!(
                "TIMESTAMP WENT BACKWARDS! new_ts={:?} ({} micros) < existing_ts={:?} ({} micros)",
                new_ts,
                new_ts_micros,
                last.ts,
                last_ts_micros,
            ));
        }
    }

    ctx.db.my_table().insert(MyTable { ts: new_ts });
    Ok(())
}
