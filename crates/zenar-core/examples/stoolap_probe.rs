//! Probe: how does Stoolap behave when two OS processes open the same file DSN?
//! Spec section 65 / 85 leaves this unverified. Re-run after any Stoolap upgrade.
//!
//! Usage (run both at once):
//!   cargo run -p zenar-core --example stoolap_probe -- writer /tmp/probe.db
//!   cargo run -p zenar-core --example stoolap_probe -- reader /tmp/probe.db

use std::thread::sleep;
use std::time::Duration;

use stoolap::Database;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (role, path) = (args[1].as_str(), args[2].as_str());
    let dsn = format!("file://{path}?sync_mode=full");
    match Database::open(&dsn) {
        Ok(db) => {
            println!("[{role}] open: OK");
            db.execute(
                "CREATE TABLE IF NOT EXISTS probe (id INTEGER PRIMARY KEY, who TEXT)",
                (),
            )
            .unwrap();
            if role == "writer" {
                let n = db.execute("INSERT INTO probe (id, who) VALUES (1, 'writer')", ());
                println!("[{role}] insert: {n:?}");
                sleep(Duration::from_secs(3));
                let rows = db
                    .query("SELECT who FROM probe ORDER BY id", ())
                    .unwrap()
                    .collect_vec()
                    .unwrap();
                println!("[{role}] rows after 3s: {}", rows.len());
            } else {
                sleep(Duration::from_secs(1));
                let rows = db
                    .query("SELECT who FROM probe ORDER BY id", ())
                    .unwrap()
                    .collect_vec()
                    .unwrap();
                println!("[{role}] rows seen by reader: {}", rows.len());
                let n = db.execute("INSERT INTO probe (id, who) VALUES (2, 'reader')", ());
                println!("[{role}] insert: {n:?}");
            }
        }
        Err(e) => println!("[{role}] open: ERROR {e}"),
    }
}
