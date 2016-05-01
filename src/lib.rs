use std::sync::{Arc, Mutex};

extern crate easydb;
use easydb::Column;
use easydb::Table;
use easydb::DbPool;

use std::collections::BTreeMap;

#[macro_use]
extern crate easy_config;
use easy_config::CFG;

#[macro_use]
extern crate easy_util;
extern crate rustc_serialize;
use rustc_serialize::json::Json;
use rustc_serialize::json::ToJson;
use std::str::FromStr;

extern crate postgres;
use postgres::{Connection, SslMode};
use postgres::types::Type;

extern crate rand;
use rand::distributions::{IndependentSample, Range};

#[macro_use]
extern crate log;

pub struct MyDbPool {
    dsn:String,
    conns:Vec<Mutex<Connection>>,
}

pub fn get_back_json(rows:postgres::rows::Rows) -> Json {
    let mut rst_json = json!("{}");
    let mut data:Vec<Json> = Vec::new();
    for row in &rows {
        let mut back_json = json!("{}");
        let columns = row.columns();
        for column in columns {
            let name = column.name();
            let col_type = column.type_();
            match *col_type {
                Type::Int4 => {
                    let op:Option<postgres::Result<i32>> = row.get_opt(name);
                    let mut true_value:i32 = 0;
                    if let Some(rst) = op {
                        if let Ok(value) = rst {
                            true_value = value;
                        }
                    }
                    json_set!(&mut back_json; name; true_value);
                },
                Type::Int8 => {
                    let op:Option<postgres::Result<i64>> = row.get_opt(name);
                    let mut true_value:i64 = 0;
                    if let Some(rst) = op {
                        if let Ok(value) = rst {
                            true_value = value;
                        }
                    }
                    json_set!(back_json; name; true_value);
                },
                Type::Varchar | Type::Text => {
                    let op:Option<postgres::Result<String>> = row.get_opt(name);
                    let mut true_value:String = String::new();
                    if let Some(rst) = op {
                        if let Ok(value) = rst {
                            true_value = value;
                        }
                    }
                    json_set!(back_json; name; true_value);
                },
                _ => {
                    println!("ignore type:{}", col_type.name());
                },
            }
        }
        data.push(back_json);
    }
    json_set!(&mut rst_json; "data"; data);
    json_set!(&mut rst_json; "rows"; rows.len());
    rst_json
}

impl MyDbPool {

    pub fn new(dsn:&str, size:u32) -> MyDbPool {
        let mut conns = vec![];
        for _ in 0..size {
            let conn = match Connection::connect(dsn, SslMode::None) {
                Ok(conn) => conn,
                Err(e) => {
                    error!("Connection error: {}", e);
                    break;
                }
            };
            conns.push(Mutex::new(conn));
        }
        MyDbPool {
            dsn:dsn.to_string(),
            conns:conns,
        }
    }

    /**
     * 获得dsn字符串
     */
    pub fn get_dsn(&self) -> String {
        self.dsn.clone()
    }

}

impl DbPool for MyDbPool {

    fn get_connection(&self) -> Result<Connection, i32> {
        let rst = match Connection::connect(self.dsn.as_str(), SslMode::None) {
            Ok(conn) => Result::Ok(conn),
            Err(e) => {
                println!("Connection error: {}", e);
                Result::Err(-1)
            }
        };
        rst
    }

    fn execute(&self, sql:&str) -> Result<Json, i32> {
        info!("{}", sql);
        let between = Range::new(0, self.conns.len());
        let mut rng = rand::thread_rng();
        let rand_int = between.ind_sample(&mut rng);
        let conn = self.conns[rand_int].lock().unwrap();

        let out_rst = {
            let rst = conn.query(sql, &[]);
            rst.and_then(|rows| {
                Result::Ok(get_back_json(rows))
            })
        };

        match out_rst {
            Ok(json) => {
                Result::Ok(json)
            },
            Err(err) => {
                println!("{}", err);
                Result::Err(-1)
            },
        }
    }
    
    fn stream<F>(&self, sql:&str, f:F) -> Result<i32, i32> where F:FnMut(Json) -> bool + 'static {
        let conn = self.get_connection().unwrap();
        stream(conn, sql, f)
	}

}

pub fn stream<F>(conn:Connection, sql:&str, mut f:F) -> Result<i32, i32> where F:FnMut(Json) -> bool + 'static {
    let rst = conn.query("BEGIN", &[]);

    //begin
    let rst = rst.and_then(|rows| {
        let json = get_back_json(rows);
        println!("{}", json);
        Result::Ok(1)
    }).or_else(|err|{
        println!("{}", err);
        Result::Err(-1)
    });

    //cursor
    let rst = rst.and_then(|_| {
		let cursor_sql = format!("DECLARE myportal CURSOR FOR {}", sql);
		println!("{}", cursor_sql);
		let rst = conn.query(&cursor_sql, &[]);
		rst.and_then(|rows|{
            let json = get_back_json(rows);
            println!("{}", json);
            Result::Ok(1)
        }).or_else(|err|{
            println!("{}", err);
            Result::Err(-1)
        })
    });

    let rst = rst.and_then(|_| {
        let fetch_sql = "FETCH NEXT in myportal";
        println!("{}", fetch_sql);

        let mut flag = 0;
        loop {
            let rst = conn.query(&fetch_sql, &[]);
            let _ = rst.and_then(|rows|{
                let json = get_back_json(rows);
                let rows = json_i64!(&json; "rows");
                if rows < 1 {
                    flag = -2;
                } else {
                    let f_back = f(json);
                    if !f_back {
                        flag = -2;
                    }
                }
                Result::Ok(flag)
            }).or_else(|err|{
                println!("{}", err);
                flag = -1;
                Result::Err(flag)
            });
            if flag < 0 {
                break;
            }
        }
        match flag {
            -1 => {
                Result::Err(-1)
            },
            _ => {
                Result::Ok(1)
            },
        }
    });

    //close the portal
    let rst = rst.and_then(|_|{
  		let close_sql = "CLOSE myportal";
        println!("{}", close_sql);
        let rst = conn.query(&close_sql, &[]);
        rst.and_then(|rows|{
            let json = get_back_json(rows);
            println!("{}", json);
            Result::Ok(1)
        }).or_else(|err|{
            println!("{}", err);
            Result::Err(-1)
        })
    });

    //end the cursor
    let rst = rst.and_then(|_|{
    	let end_sql = "END";
        println!("{}", end_sql);
        let rst = conn.query(&end_sql, &[]);
        rst.and_then(|rows|{
            let json = get_back_json(rows);
            println!("{}", json);
            Result::Ok(1)
        }).or_else(|err|{
            println!("{}", err);
            Result::Err(-1)
        })		
   	});
    rst
}


pub struct DataBase {
    pub name:String,
    pub table_list:BTreeMap<String, Table<MyDbPool>>,
    pub dc:Arc<MyDbPool>,   //data center
}

impl DataBase {

    pub fn get_connection(&self) -> Result<Connection, i32> {
        self.dc.get_connection()
    }

    fn get_table_define(name:&str, vec:Vec<Column>, dc:Arc<MyDbPool>) -> Table<MyDbPool>
    {
        let mut map = BTreeMap::new();
        for col in vec {
            map.insert(col.name.clone(), col);
        }
        Table::new(name, map, dc)
    }

    pub fn new() -> DataBase
    {
        info!(target:"main", "{}", CFG.get_data());
        let dsn = cfg_str!("db", "dsn");
        let name = cfg_str!("db", "name");
        let pool:MyDbPool = MyDbPool::new(dsn, cfg_i64!("db", "conn_limit") as u32);
        let dc = Arc::new(pool);
    
        let table_list = DataBase::get_tables_from_cfg(dc.clone());
        for (_, table) in table_list.iter() {
            info!("{}", table.to_ddl_string());
        }
        DataBase {
            name:name.to_string(),
            table_list:table_list,
            dc:dc,
        }
    }
    
    //get the table define from the cfg file
    fn get_tables_from_cfg(dc:Arc<MyDbPool>) -> BTreeMap<String, Table<MyDbPool>> {
        let mut table_list = BTreeMap::new();
        let tables = cfg_path!("db", "tables");
        let table_array = tables.as_array().unwrap();
        for table in table_array {
            let dc = dc.clone();
            let name = json_str!(table; "name");
            let cols_json = json_path!(table; "cols");
            let cols_array = cols_json.as_array().unwrap();
            let mut cols_vec = Vec::new();
            for col in cols_array {
                let name = json_str!(col; "name");
                let col_type = json_str!(col; "type");
                let length = json_i64!(col; "length");
                let des = json_str!(col; "des");
                let escape = col.find("escape").unwrap().as_boolean().unwrap();
                let column = Column::new(name, col_type, length as i32, des, escape);
                cols_vec.push(column);
            }
            let cur_table = DataBase::get_table_define(name, cols_vec, dc);
            table_list.insert(name.to_string(), cur_table);
        }
        table_list
    }

    pub fn get_table(&self, name:&str) -> Option<&Table<MyDbPool>>
    {
        self.table_list.get(name)
    }

    pub fn execute(&self, sql:&str) -> Result<Json, i32> {
        self.dc.execute(&sql)
    }
	
	pub fn stream<F>(&self, sql:&str, f:F) -> Result<i32, i32> where F:FnMut(Json) -> bool + 'static {
		self.dc.stream(sql, f)
	}
}

