use json;
use std::collections::{HashMap, HashSet};
use json::JsonValue;
use json::number::Number;
use avro_rs::Schema;
use avro_rs::schema::{Name, UnionSchema, RecordField, RecordFieldOrder, SchemaKind};
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

pub fn merge_schemas(schema1: Schema, schema2: Schema) -> Schema {
    match (schema1, schema2) {
        (Schema::Record {name: name1, doc: doc1, fields: mut fields1, lookup: lookup1},
            Schema::Record {name: name2, doc: doc2, fields: mut fields2, lookup: mut lookup2}) => {
            let mut all_fields = HashMap::new();
            for field in fields1 {
                all_fields.insert((&field).name.clone(), vec![field]);
            }

            for field in fields2 {
                if all_fields.contains_key(&field.name) {
                    all_fields.get_mut(&field.name).unwrap().push(field);
                } else {
                    all_fields.insert((&field).name.clone(), vec![field]);
                }
            }

            let mut merged_fields = Vec::new();
            for (field_name, mut fields) in all_fields {
                if fields.len() == 1 {
                    let mut field = fields.pop().unwrap();
                    let schema = Schema::Union(UnionSchema::new(vec![Schema::Null, (&field).schema.clone()]).unwrap());
                    field.position = merged_fields.len();
                    field.schema = schema;
                    field.default = Some(Value::Null);
                    merged_fields.push(field);
                } else {
                    let (mut field1, field2) = (fields.pop().unwrap(), fields.pop().unwrap());
                    let merged_schema = merge_schemas((&field1).schema.clone(), field2.schema);
                    field1.schema = merged_schema;
                    field1.position = merged_fields.len();
                    merged_fields.push(field1);
                }
            }

            let lookup: HashMap<String, usize> = merged_fields.iter().enumerate().map(|(i, field)| (field.name.clone(), i)).collect();
            Schema::Record {name: name1, doc: doc1, fields: merged_fields, lookup}
        }
        (Schema::Union(us1), Schema::Union(us2)) => {
            let mut schema_kinds: HashMap<SchemaKind, Vec<Schema>> = HashMap::new();
            for schema in us1.variants().to_vec() {
                let sk = SchemaKind::from(&schema);
                schema_kinds.insert(sk, vec![schema]);
            }

            for schema in us2.variants().to_vec() {
                let sk = SchemaKind::from(&schema);
                if schema_kinds.contains_key(&sk) {
                    schema_kinds.get_mut(&sk).unwrap().push(schema);
                } else {
                    schema_kinds.insert(sk, vec![schema]);
                }
            }

            let mut merged_schemas = Vec::new();

            if let Some(_) = schema_kinds.remove(&SchemaKind::Null) {
                merged_schemas.push(Schema::Null);
            }
            for (_, mut schemas) in schema_kinds {
                if schemas.len() == 1 {
                    merged_schemas.push(schemas.pop().unwrap());
                } else {
                    merged_schemas.push(merge_schemas(schemas.pop().unwrap(), schemas.pop().unwrap()));
                }
            }

            Schema::Union(UnionSchema::new(merged_schemas).unwrap())
        }
        (Schema::Array(schema1), Schema::Array(schema2)) => {
            let merged_schema = merge_schemas(*schema1, *schema2);
            Schema::Array(Box::new(merged_schema))
        }
        (Schema::Map(schema1), Schema::Map(schema2)) => {
            let merged_schema = merge_schemas(*schema1, *schema2);
            Schema::Map(Box::new(merged_schema))
        }
        (s1, s2) if SchemaKind::from(&s1) == SchemaKind::from(&s2) => {
            s1
        }
        (s1, s2) => {
            Schema::Union(UnionSchema::new(vec![s1, s2]).unwrap())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_schema() {
        let schema1 = Schema::parse_str(r#"{
     "type": "record",
     "namespace": "com.example",
     "name": "FullName",
     "fields": [
       { "name": "first", "type": "string" },
       { "name": "last", "type": "string" }
     ]
} "#).unwrap();

        let schema2 = Schema::parse_str(r#"{
     "type": "record",
     "namespace": "com.example",
     "name": "FullName",
     "fields": [
       { "name": "first", "type": "int" }
     ]
} "#).unwrap();

        let merged_schema = merge_schemas(schema1.clone(), schema2);
        println!("{:?}", &merged_schema);
    }
}