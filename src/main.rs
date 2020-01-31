mod io;
mod avro;

use json;
use std::ptr::null;
use crate::io::GzipFile;
use std::time::{Instant, Duration};
use std::borrow::Borrow;
use serde_json;
use serde_json::{Value, Map, Deserializer};
use simd_json;
use std::thread;
use dns_lookup;
use std::net::{IpAddr, Ipv4Addr};
use std::str::FromStr;
use simd_json::value::{Value as SimdValue};
use flate2::{write::DeflateEncoder, Compression};
use std::io::{Write, BufRead};
use libdeflater::{Compressor, CompressionLvl};
use deflate::deflate_bytes;
use json::JsonValue;
use json::number::Number;

#[macro_use] extern crate lazy_static;


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
    let mut json_file = GzipFile::new("TweetsChampions.json.gz");
    let now = Instant::now();
    for line_result in json_file.lines.into_iter() {
        if let Ok(line) = line_result {
            let x: Value = serde_json::from_str(line.as_ref()).unwrap();
        }
    }
    println!("Execution time: {:?}", now.elapsed().as_millis());
}

fn simd_benchmark() {
    let json_file = GzipFile::new("TweetsChampions.json.gz");
    let now = Instant::now();
    for line_result in json_file.lines {
        if let Ok(mut line) = line_result {
//            let v = unsafe { simd_json::to_borrowed_value(line.as_bytes_mut()).unwrap() };
            let tape = unsafe { simd_json::to_tape(line.as_bytes_mut()).unwrap()};
            println!("{:?}", tape);
            break
        }
    }
    println!("Execution time: {:?}", now.elapsed().as_millis());
}

fn flate2_benchmark() {
    let json_file = GzipFile::new("TweetsChampions.json.gz");
    let now = Instant::now();
    json_file
        .lines
        .for_each(|line| {
            let mut encoder = DeflateEncoder::new(Vec::new(), Compression::new(6));
            encoder.write_all(line.unwrap().as_bytes());
            let v = encoder.finish().unwrap();
        });
    println!("Execution time: {:?}", now.elapsed().as_millis());
}

fn libflater_benchmark() {
    let json_file = GzipFile::new("TweetsChampions.json.gz");
    let now = Instant::now();
    let mut dc = libdeflater::Compressor::new(CompressionLvl::new(6).unwrap());
    json_file
        .lines
        .for_each(|line| {
            let l = line.unwrap();
            let bytes = l.as_bytes();
            let mut v = Vec::new();
            v.resize(dc.deflate_compress_bound(bytes.len()), 0);
            dc.deflate_compress(bytes, &mut v).unwrap();
        });
    println!("Execution time: {:?}", now.elapsed().as_millis());
}

fn deflate_benchmark() {
    let json_file = GzipFile::new("TweetsChampions.json.gz");
    let now = Instant::now();
    json_file
        .lines
        .for_each(|line| {
            let b = deflate_bytes(line.unwrap().as_bytes());
        });
    println!("Execution time: {:?}", now.elapsed().as_millis());
}

fn main() {
//    json_benchmark();
//    serde_benchmark();
//    simd_benchmark();
//    flate2_benchmark();
//    libflater_benchmark();
//    deflate_benchmark();
    println!("{:?}", JsonValue::Number(Number::from(123)).as_fixed_point_i64(0));
}
