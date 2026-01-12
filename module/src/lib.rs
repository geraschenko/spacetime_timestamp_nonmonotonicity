//! Minimal reproduction case for SpacetimeDB timestamp collision.
//!
//! This module demonstrates that ctx.timestamp can collide when
//! multiple reducer calls happen within the same microsecond.

use spacetimedb::{reducer, table, ReducerContext, Table, Timestamp};

#[table(name = my_table, public)]
pub struct MyTable {
    #[primary_key]
    pub ts: Timestamp,
}

#[reducer]
pub fn insert_row(ctx: &ReducerContext) -> Result<(), String> {
    let new_ts = ctx.timestamp;
    let new_ts_micros = new_ts.to_micros_since_unix_epoch();

    // Check against the most recent entry
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
