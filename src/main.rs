mod io;
mod avro;

use json;
use std::ptr::null;
use crate::io::GzipFile;
use std::time::{Instant, Duration};
use std::borrow::Borrow;
use serde_json;
use serde_json::{Value, Map};
use simd_json;
use std::thread;


fn json_benchmark() {
    let json_file = GzipFile::new("TweetsChampions.json.gz");
    let now = Instant::now();
    json_file
        .lines
        .map(|x| x.unwrap())
        .for_each(|x| {json::parse(x.as_str());});
    println!("Execution time: {:?}", now.elapsed().as_millis());
}


fn serde_benchmark() {
    let json_file = GzipFile::new("TweetsChampions.json.gz");
    let now = Instant::now();
    json_file
        .lines
        .map(|x| x.unwrap())
        .for_each(|x| {let x: Value = serde_json::from_str(x.as_str()).unwrap();});
    println!("Execution time: {:?}", now.elapsed().as_millis());
}

fn simd_benchmark() {
    let json_file = GzipFile::new("TweetsChampions.json.gz");
    let now = Instant::now();
    json_file
        .lines
        .map(|x| x.unwrap())
        .for_each(|mut x| {let x: Value = simd_json::serde::from_str(x.as_mut_str()).unwrap();});
    println!("Execution time: {:?}", now.elapsed().as_millis());
}

fn main() {
//    json_benchmark();
////    serde_benchmark();
//    simd_benchmark();
    let json = r#"{"a":1, "b": [1], "c": {"d": true}}"#;
    let schema = avro::infer_schema(&json::parse(json).unwrap());
    println!("{:?}", &schema);
}
