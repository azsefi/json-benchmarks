use json;
use std::collections::{HashMap, HashSet};
use json::JsonValue;
use json::number::Number;
use avro_rs::Schema;
use avro_rs::schema::{Name, UnionSchema, RecordField, RecordFieldOrder};
use serde_json;
use serde_json::{Value, Map};
use failure::Error;


fn get_type(json_value: &JsonValue, name: &str) -> Value {
    if json_value.is_boolean() {
        Value::String("boolean".to_string())
    }
    else if json_value.is_string() {
        Value::String("string".to_string())
    }
    else if json_value.is_number() {
        let (_, mantissa, exponent) = json_value.as_number().unwrap().as_parts();
        if exponent == 0 {
            Value::String("long".to_string())
        }
        else {
            Value::String("double".to_string())
        }
    }
    else if json_value.is_empty() {
        Value::String("null".to_string())
    }
    else if json_value.is_array() {
        let element_type_set: Vec<Value> =
            json_value
                .members()
                .map(|array_element| get_type(array_element, name))
                .collect();

        let mut element_types: Vec<Value>=
            element_type_set
                .into_iter()
                .take(1) //ToDo: element_type_set must be HashSet and take(1) must be removed
                .collect();

        let items_type =
            if element_types.len() == 1 {
                element_types.pop().unwrap()
            }
            else if element_types.len() == 0 {
                Value::String("null".to_string())
            }
            else {
                Value::Array(element_types)
            };

        let mut array_type = serde_json::Map::new();
        array_type.insert("type".to_owned(), Value::String("array".to_owned()));
        array_type.insert("items".to_owned(), items_type);
        Value::Object(array_type)
    }
    else if json_value.is_object() {
        infer_record_schema(json_value, name.to_owned()).unwrap()
    }
    else {
        Value::String("null".to_string())
    }
}

fn infer_record_schema(json_value: &JsonValue, record_name: String) -> Option<Value> {
    if json_value.is_object() {
        let fields: Vec<Value> =
            json_value
                .entries()
                .enumerate()
                .map(|(idx, (field_name, value))| {
                    let mut field = serde_json::Map::new();
                    field.insert("name".to_owned(), Value::String(field_name.to_owned()));
                    let field_type = get_type(value, field_name);
                    field.insert("type".to_owned(), field_type);
                    Value::Object(field)
                })
                .collect();
        let mut record_schema = serde_json::Map::new();
        record_schema.insert("name".to_owned(), Value::String(record_name.clone()));
        record_schema.insert("type".to_owned(), Value::String("record".to_owned()));
        record_schema.insert("fields".to_owned(), Value::Array(fields));

        Some(Value::Object(record_schema))
    }
    else {None}
}

pub fn infer_schema(json_value: &JsonValue) -> Result<Schema, Error> {
    let schema_value = infer_record_schema(json_value, "inferred_schema".to_owned());
    Schema::parse(schema_value.as_ref().unwrap())
}