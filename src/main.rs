extern crate dc;
use dc::{MyDbPool, DataBase, stream};

use std::sync::Arc;

#[macro_use]
extern crate log;
use log::{LogRecord, LogLevel, LogMetadata, SetLoggerError, LogLevelFilter};

extern crate chrono;
use chrono::*;

extern crate colored;
use colored::*;

#[macro_use]
extern crate easy_config;
use easy_config::CFG;

#[macro_use]
extern crate easy_util;
extern crate rustc_serialize;
use rustc_serialize::json::Json;
use rustc_serialize::json::ToJson;
use std::str::FromStr;

pub struct SimpleLogger {
    target: String,
}

impl SimpleLogger {

    pub fn new() -> SimpleLogger {
        let target = cfg_str!("target");
        SimpleLogger {
            target: target.to_string(),
        }
    }

}

impl log::Log for SimpleLogger {

    fn enabled(&self, metadata: &LogMetadata) -> bool {
        if self.target == "run" {
            metadata.level() <= LogLevel::Info
        } else {
            metadata.level() <= LogLevel::Info
        }
    }

    fn log(&self, record: &LogRecord) {
        let mt = record.metadata();
        if self.enabled(mt) {
            let head = format!("[{}] [{}] [{}]", Local::now(), record.level(), mt.target());
            let colored_head;
            match record.level() {
                LogLevel::Error => {
                    colored_head = head.red();
                },
                LogLevel::Debug => {
                    colored_head = head.yellow();
                },
                _ => {
                    colored_head = head.green();
                }
            }
            println!("{} {}", colored_head, record.args());
        }
    }
}

pub fn init() -> Result<(), SetLoggerError> {
    let target = cfg_str!("target");
    log::set_logger(|max_log_level| {
        if target == "run" {
            max_log_level.set(LogLevelFilter::Info);
        } else {
            max_log_level.set(LogLevelFilter::Trace);
        }
        Box::new(SimpleLogger::new())
    })
}

fn main() {
	let _ = init();
    let my_db = DataBase::new();
	
    let rst = my_db.execute("select * from customer");
    let _ = rst.and_then(|json| {
        println!("{}", json);
        Result::Ok(())
    });
    
    let conn = my_db.get_connection().unwrap();

    let db = DataBase::new_with_limit(1);
    let _ = stream(conn, "select * from customer", move |json| {
        let rst = db.execute("select * from customer");
        println!("{}", json);
        true
    });
}
