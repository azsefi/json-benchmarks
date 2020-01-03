mod io;

use json;
use std::collections::{HashMap, HashSet};
use json::JsonValue;
use json::number::Number;
use avro_rs::Schema;
use avro_rs::schema::{Name, UnionSchema, RecordField, RecordFieldOrder};
use std::ptr::null;
use crate::io::GzipFile;
use std::time::Instant;
use std::borrow::Borrow;
use serde_json;
use serde_json::Value;
use simd_json;

fn get_type(jsonValue: &JsonValue) -> Schema {
    if jsonValue.is_boolean() {
        Schema::Boolean
    }
    else if jsonValue.is_string() {
        Schema::String
    }
    else if jsonValue.is_number() {
        let (_, mantissa, exponent) = jsonValue.as_number().unwrap().as_parts();
        if exponent == 0 {
            Schema::Long
        }
        else {
            Schema::Double
        }
    }
    else if jsonValue.is_empty() {
        Schema::Null
    }
//    else if json.is_array() {
//        let element_type_set: HashSet<Schema> = json.members().map(|array_element| get_type(array_element)).collect();
//        let mut element_types: Vec<&Schema>= element_type_set.iter().collect();
//        if element_types.len() == 1 {
//            element_types.pop().unwrap().to_owned()
//        }
//        else if element_types.len() == 0 {
//            Schema::Null
//        }
//        else {
//            let mut elements = Vec::new();
//            element_types.
//                iter().
//                map(|t| t.to_owned().to_owned()).
//                for_each(|t| {elements.push(t);});
//
//            Schema::Union(UnionSchema{schemas: elements, variant_index: Default::default() })
//        }
//    }
    else {
        Schema::Null
    }
}

//fn infer_schema(json: &JsonValue) -> String {
//    let mut schema = JsonValue::new_object();
//    let entries = json.entries();
//    entries.for_each(|(field_name, value)| {schema.insert(field_name, get_type(value));});
//    schema.to_string()
//}

fn infer_schema2(json: &JsonValue, record_name: String) -> Option<Schema> {
    if json.is_object() {
        let fields: Vec<RecordField> =
            json.entries().enumerate().map(|(idx, (field_name, value))| RecordField{
                name: field_name.to_string(),
                doc: None,
                default: None,
                schema: get_type(value),
                order: RecordFieldOrder::Ascending,
                position: idx
            }).collect();
        Some(Schema::Record {
            name: Name{ name: record_name, namespace: None, aliases: None },
            doc: None, fields: fields,
            lookup: Default::default() })
    }
    else {None}
}

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
    json_benchmark();
//    serde_benchmark();
    simd_benchmark()
}
