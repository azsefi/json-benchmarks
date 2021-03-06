use json;
use std::collections::{HashMap, HashSet};
use json::JsonValue;
use json::number::Number;
use avro_rs::types::{Value as AvroValue, Record};
use avro_rs::Schema;
use avro_rs::schema::{Name, UnionSchema, RecordField, RecordFieldOrder, SchemaKind};
use serde_json;
use serde_json::{Value, Map};
use failure::Error;
use std::fs::File;
use flate2::read::GzDecoder;
use std::io::{BufReader, BufRead, Lines};
use regex::Regex;
use std::borrow::{Cow, BorrowMut};
use std::ops::{DerefMut, Deref};
use std::iter::FromIterator;


lazy_static! {
    static ref PRIMITIVES: HashSet<SchemaKind> = HashSet::from_iter(vec![SchemaKind::Null, SchemaKind::String, SchemaKind::Boolean, SchemaKind::Double, SchemaKind::Long, SchemaKind::Bytes, SchemaKind::Float, SchemaKind::Int]);
    static ref DUMMY_FIELD: RecordField = RecordField{
                name: "".to_string(),
                doc: None,
                default: None,
                schema: Schema::Null,
                order: RecordFieldOrder::Ascending,
                position: 0
            };
}


pub struct GzipFile {
    pub lines: Lines<BufReader<GzDecoder<File>>>
}

impl GzipFile {
    pub fn new(file_path: &str) -> Self {
        let file = File::open(file_path).unwrap();
        let lines = GzDecoder::new(file);
        let buf_reader = BufReader::new(lines);
        let lines: Lines<BufReader<GzDecoder<File>>> = buf_reader.lines();
        GzipFile{lines}
    }
}


pub fn infer_schema_serde(json_value: Value, name: &str) -> Result<Schema, Error> {
    match json_value {
        Value::Bool(_) => Ok(Schema::Boolean),
        Value::String(_) => Ok(Schema::String),
        Value::Number(number) => {
            if number.is_u64() || number.is_i64() {
                Ok(Schema::Long)
            }
            else {
                Ok(Schema::Double)
            }
        },
        Value::Array(mut vector) => {
            let items_schema =
                if let Some(first_element) = vector.pop() {
                    let initial_schema = infer_schema_serde(first_element, name);
                    vector
                        .into_iter()
                        .fold(initial_schema, |base, element| {
                            let schema = infer_schema_serde(element, name)?;
                            merge_schemas(base?, schema)
                        })
                } else {
                    // if array is empty then type is null
                    Ok(Schema::Null)
                };

            Ok(Schema::Array(Box::new(items_schema?)))
        },
        Value::Object(obj) => {
            let mut fields = Vec::new();
            for (field_name, field_value) in obj{
                let field_schema = infer_schema_serde(field_value, &field_name)?;
                let record_field = RecordField{
                    name: field_name,
                    doc: None,
                    default: None,
                    schema: field_schema,
                    order: RecordFieldOrder::Ascending,
                    position: fields.len()
                };
                fields.push(record_field);
            }
            Ok(Schema::Record {
                name: Name {
                    name: name.to_owned(),
                    namespace: None,
                    aliases: None
                },
                doc: None,
                fields,
                lookup: Default::default()
            })
        },
        Value::Null => Ok(Schema::Null)
    }
}


pub fn infer_schema(json_value: &JsonValue, name: &str) -> Result<Schema, Error> {
    match json_value {
        JsonValue::Boolean(_) => { Ok(Schema::Boolean) },
        JsonValue::String(_) => { Ok(Schema::String) },
        JsonValue::Number(number) => {
            let (_, mantissa, exponent) = number.as_parts();
            if exponent == 0 {
                Ok(Schema::Long)
            }
            else {
                Ok(Schema::Double)
            }
        },
        JsonValue::Array(vector) => {
            let items_schema =
                if let Some((first_element, rest)) = vector.split_first() {
                    let initial_schema = infer_schema(first_element, name);
                    rest
                        .iter()
                        .fold(initial_schema, |base, element| {
                            let schema = infer_schema(element, name)?;
                            merge_schemas(base?, schema)
                        })
                } else {
                    // if array is empty then type is null
                    Ok(Schema::Null)
                };

            Ok(Schema::Array(Box::new(items_schema?)))
        },
        JsonValue::Object(obj) => {
            let mut fields = Vec::new();
            for (field_name, field_value) in json_value.entries() {
                let field_schema = infer_schema(field_value, field_name)?;
                let record_field = RecordField{
                    name: field_name.to_owned(),
                    doc: None,
                    default: None,
                    schema: field_schema,
                    order: RecordFieldOrder::Ascending,
                    position: fields.len()
                };
                fields.push(record_field);
            }

            let lookup: HashMap<String, usize> =
                fields
                    .iter()
                    .enumerate()
                    .map(|(i,f)| (f.name.clone(), i))
                    .collect();

            Ok(Schema::Record {
                name: Name::new(name),
                doc: None,
                fields,
                lookup
            })
        },
        JsonValue::Null => { Ok(Schema::Null) },
        JsonValue::Short(_) => { Ok(Schema::Long) },
        _ => { Ok(Schema::Null) }
    }
}


pub fn merge_schemas(schema1: Schema, schema2: Schema) -> Result<Schema, Error> {
    match (schema1, schema2) {
        (Schema::Record {name,  doc, fields: mut fields1, mut lookup},
            Schema::Record {name: _, doc: _, fields: mut fields2, lookup: mut lookup2}) => {
//            let mut merged_fields = Vec::new();
            for mut field1 in fields1.iter_mut() {
                let schema2 =
                    lookup2
                        .remove(&field1.name)
                        .map(|idx2| std::mem::replace(&mut fields2[idx2].schema, Schema::Null))
                        .unwrap_or(Schema::Null);

                let merged_schema = merge_schemas(std::mem::replace(field1.schema.borrow_mut(), Schema::Null), schema2)?;
                field1.schema = merged_schema;
//                merged_fields.push(field1);
            }

            for (field_name, idx2) in lookup2 {
                let mut field2 = std::mem::replace(&mut fields2[idx2], DUMMY_FIELD.clone());
                field2.position = fields1.len();
                lookup.insert(field_name, fields1.len());
                field2.schema = merge_schemas(Schema::Null, field2.schema)?;
                fields1.push(field2);
            }

//            let mut all_fields: HashMap<String, Vec<RecordField>> =
//                fields1
//                    .into_iter()
//                    .map(|field| ((&field).name.clone(), vec![field]))
//                    .collect();
//
//            for field in fields2 {
//                all_fields
//                    .entry((&field).name.clone())
//                    .or_insert(vec![])
//                    .push(field);
//            }
//
//            let mut merged_fields = Vec::with_capacity(all_fields.len());
//            for (field_name, mut fields) in all_fields {
//                let mut field = fields.pop().unwrap();
//                let s1 = field.schema;
//                let s2 = fields.pop().map(|f| f.schema).unwrap_or(Schema::Null);
//                let merged_schema = merge_schemas(s1, s2)?;
//                field.position = merged_fields.len();
//                field.schema = merged_schema;
//                merged_fields.push(field);
//            }

            Ok(Schema::Record {name, doc, fields: fields1, lookup})
        }
        (Schema::Union(mut us1), Schema::Union(mut us2)) => {
            let mut schema_kinds: HashMap<SchemaKind, Vec<Schema>> = HashMap::new();
            while let Some(schema) = us1.variants_mut().pop() {
                let sk = SchemaKind::from(&schema);
                schema_kinds.insert(sk, vec![schema]);
            }

            while let Some(schema) = us2.variants_mut().pop() {
                let sk = SchemaKind::from(&schema);
                schema_kinds
                    .entry(sk)
                    .or_insert(vec![])
                    .push(schema);
            }

            let mut merged_schemas = Vec::new();
            if let Some(_) = schema_kinds.remove(&SchemaKind::Null) {
                merged_schemas.push(Schema::Null);
            }

            for (sk, mut schemas) in schema_kinds {
                if schemas.len() == 1 || PRIMITIVES.contains(&sk) {
                    merged_schemas.push(schemas.pop().unwrap());
                } else {
                    merged_schemas.push(merge_schemas(schemas.pop().unwrap(), schemas.pop().unwrap())?);
                }
            }

            Ok(Schema::Union(UnionSchema::new(merged_schemas)?))
        }
        (Schema::Union(mut us1), s2 ) => {
//            let mut schema_kinds = HashMap::new();
//            while let Some(schema) = us1.variants_mut().pop() {
//                let sk = SchemaKind::from(&schema);
//                schema_kinds.insert(sk, vec![schema]);
//            }
//
//            schema_kinds
//                .entry(SchemaKind::from(&s2))
//                .or_insert(vec![])
//                .push(s2);
//
//            let mut merged_schemas = Vec::new();
//            let sk = SchemaKind::from(&Schema::Null);
//            if schema_kinds.remove(&sk).is_some() {
//                merged_schemas.push(Schema::Null);
//            }
//            for (sk, mut schemas) in schema_kinds {
//                if schemas.len() == 1 || PRIMITIVES.contains(&sk) {
//                    merged_schemas.push(schemas.pop().unwrap());
//                } else {
//                    merged_schemas.push(merge_schemas(schemas.pop().unwrap(), schemas.pop().unwrap())?);
//                }
//            }
//
//            Ok(Schema::Union(UnionSchema::new(merged_schemas)?))

            let sk = SchemaKind::from(&s2);
            if let Some((i, s1)) = us1.find_schema_kind_mut(&sk) {
                let s1 = std::mem::replace(s1, Schema::Null);
                let merged_schema = merge_schemas(s1, s2);
                us1.variants_mut()[i] = merged_schema?;
            } else {
                us1.variants_mut().push(s2);
            }

            Ok(Schema::Union(us1))
        }
        (s2, Schema::Union(mut us1) ) => {
//            let mut schema_kinds = HashMap::with_capacity(us1.variants().len()+1);
//            while let Some(schema) = us1.variants_mut().pop() {
//                let sk = SchemaKind::from(&schema);
//                schema_kinds.insert(sk, vec![schema]);
//            }
//
//            schema_kinds
//                .entry(SchemaKind::from(&s2))
//                .or_insert(vec![])
//                .push(s2);
//
//            let mut merged_schemas = Vec::new();
//            let sk = SchemaKind::from(&Schema::Null);
//            if schema_kinds.remove(&sk).is_some() {
//                merged_schemas.push(Schema::Null);
//            }
//            for (sk, mut schemas) in schema_kinds {
//                if schemas.len() == 1 || PRIMITIVES.contains(&sk) {
//                    merged_schemas.push(schemas.pop().unwrap());
//                } else {
//                    merged_schemas.push(merge_schemas(schemas.pop().unwrap(), schemas.pop().unwrap())?);
//                }
//            }
//
//            Ok(Schema::Union(UnionSchema::new(merged_schemas)?))
            let sk = SchemaKind::from(&s2);
            if let Some((i, s1)) = us1.find_schema_kind_mut(&sk) {
                let s1 = std::mem::replace(s1, Schema::Null);
                let merged_schema = merge_schemas(s1, s2);
                us1.variants_mut()[i] = merged_schema?;
            } else {
                us1.variants_mut().push(s2);
            }

            Ok(Schema::Union(us1))
        }
        (Schema::Array(schema1), Schema::Array(schema2)) => {
            let merged_schema = merge_schemas(*schema1, *schema2)?;
            Ok(Schema::Array(Box::new(merged_schema)))
        }
        (Schema::Map(schema1), Schema::Map(schema2)) => {
            let merged_schema = merge_schemas(*schema1, *schema2)?;
            Ok(Schema::Map(Box::new(merged_schema)))
        }
        (s1, s2) if SchemaKind::from(&s1) == SchemaKind::from(&s2) => {
            Ok(s1)
        }
        (s1, s2) => {
            let schemas =
                if s1 == Schema::Null {
                    vec![s1,s2]
                } else {
                    vec![s2,s1]
                };
            Ok(Schema::Union(UnionSchema::new(schemas)?))
        }
    }
}

//
//fn clean_name(txt: &str) -> String {
//    let re = Regex::new(r"[^A-Za-z\d]").unwrap();
//    let pre_clean = re.replace_all(txt, "_").to_string();
//
//    let re = Regex::new(r"__+").unwrap();
//    let mut clean =
//        re
//            .replace_all(pre_clean.as_str(), "_")
//            .trim_start_matches("_")
//            .trim_end_matches("_")
//            .to_string();
//
//    if let Some(c) = clean.chars().next() {
//        if c.is_numeric() {
//            clean = "_".to_owned() + clean.as_str()
//        }
//    }
//    clean
//}
//
//
//pub fn json_to_avro(mut json: JsonValue, schema: Schema) -> Result<AvroValue, Error> {
//    let sk = SchemaKind::from(schema);
//    match (json, schema) {
//        (JsonValue::String(s), _) => { Ok(AvroValue::String(s)) }
//        (JsonValue::Number(n), _) => {
//            if let Some(l) = n.as_fixed_point_i64(0) {
//                Ok(AvroValue::Long(l))
//            }
//            else {
//                let (sign, mantissa, exp) = n.as_parts();
//                let v = mantissa as f64 * 10_f64.powi(exp as i32) * (sign as i8 * 2 - 1) as f64;
//                Ok(AvroValue::Double(v))
//            }
//        }
//        (JsonValue::Null, _) => { Ok(AvroValue::Null) }
//        (JsonValue::Boolean(b), _) => { Ok(AvroValue::Boolean(b)) }
//        (JsonValue::Short(s), _) => { Ok(AvroValue::String(s.to_string())) }
//        (JsonValue::Array(vector), _) => {
//            let mut avro_values = Vec::with_capacity(vector.len());
//            for item in vector {
//                avro_values.push(json_to_avro(item)?);
//            }
//            Ok(AvroValue::Array(avro_values))
//        }
//        (JsonValue::Object(_), Schema::Record {fields, ..}) => {
//            let mut record_fields = Vec::new();
//            for field in fields {
//                let json = json.remove(field.name.as_str());
//                let avro = json_to_avro(json, field.schema)?;
//                record_fields.push((field.name, avro));
//            }
//
//            Ok(AvroValue::Record(record_fields))
//        }
//        _ => Ok(AvroValue::Null)
//    }
//}


//fn clean_json(json_value: &mut JsonValue) {
//    let mut new_json = JsonValue::new_object();
//    for (field_name, field_value) in json_value.entries_mut() {
//        let clean = clean_name(field_name);
//        let field_value = json_value.remove(field_name);
//        new_json.insert(clean.as_str(), field_value);
//    }
//}


#[cfg(test)]
mod test {
    use super::*;
    use std::time::Instant;
    use avro_rs::{Writer, Codec};

//    #[test]
//    fn test_clean_name() {
//        let name = "**1abc*(de&&";
//        let clean = clean_name(name);
//        assert_eq!(clean, "_1abc_de")
//    }

    #[test]
    fn test_infer_schema() {
        let json = r#"{"created_at":"Sat May 26 19:00:53 +0000 2018","id":1000451726212714496,"id_str":"1000451726212714496","text":"RT @ThaiLFC: \ud83d\ude4c \u0e40\u0e23\u0e32\u0e04\u0e37\u0e2d\u0e25\u0e34\u0e40\u0e27\u0e2d\u0e23\u0e4c\u0e1e\u0e39\u0e25\u0e25\u0e25\u0e25\u0e25\u0e25\u0e25!!!!!!\n\n#WeAreLiverpool #UCLfinal https:\/\/t.co\/e86MUogrZt","source":"\u003ca href=\"http:\/\/twitter.com\/download\/android\" rel=\"nofollow\"\u003eTwitter for Android\u003c\/a\u003e","truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user":{"id":399193055,"id_str":"399193055","name":"\u25cbSP \u30df\u30f3\ud83c\udf3b\u25cb","screen_name":"speirmint2828","location":null,"url":null,"description":"don't wake me, I'm not dreaming. \ud83c\udf43\ud83c\udf42 \u0e44\u0e21\u0e48\u0e15\u0e49\u0e2d\u0e07\u0e2a\u0e32\u0e23\u0e30\u0e41\u0e19\u0e41\u0e04\u0e1b\u0e04\u0e48\u0e30 \u0e40\u0e21\u0e19\u0e0a\u0e31\u0e48\u0e19\u0e21\u0e32","translator_type":"regular","protected":false,"verified":false,"followers_count":1416,"friends_count":225,"listed_count":10,"favourites_count":7462,"statuses_count":220056,"created_at":"Thu Oct 27 03:59:17 +0000 2011","utc_offset":null,"time_zone":null,"geo_enabled":false,"lang":"th","contributors_enabled":false,"is_translator":false,"profile_background_color":"ACDED6","profile_background_image_url":"http:\/\/abs.twimg.com\/images\/themes\/theme14\/bg.gif","profile_background_image_url_https":"https:\/\/abs.twimg.com\/images\/themes\/theme14\/bg.gif","profile_background_tile":true,"profile_link_color":"048504","profile_sidebar_border_color":"FFFFFF","profile_sidebar_fill_color":"EFEFEF","profile_text_color":"333333","profile_use_background_image":true,"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/998156737818411008\/2Zb3OxEF_normal.jpg","profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/998156737818411008\/2Zb3OxEF_normal.jpg","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/399193055\/1522692044","default_profile":false,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},"geo":null,"coordinates":null,"place":null,"contributors":null,"retweeted_status":{"created_at":"Sat May 26 18:57:35 +0000 2018","id":1000450896738725890,"id_str":"1000450896738725890","text":"\ud83d\ude4c \u0e40\u0e23\u0e32\u0e04\u0e37\u0e2d\u0e25\u0e34\u0e40\u0e27\u0e2d\u0e23\u0e4c\u0e1e\u0e39\u0e25\u0e25\u0e25\u0e25\u0e25\u0e25\u0e25!!!!!!\n\n#WeAreLiverpool #UCLfinal https:\/\/t.co\/e86MUogrZt","display_text_range":[0,57],"source":"\u003ca href=\"http:\/\/twitter.com\/download\/iphone\" rel=\"nofollow\"\u003eTwitter for iPhone\u003c\/a\u003e","truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user":{"id":904718268,"id_str":"904718268","name":"LFC Thailand","screen_name":"ThaiLFC","location":"Bangkok","url":"http:\/\/thailand.liverpoolfc.com\/","description":"\u0e17\u0e27\u0e34\u0e15\u0e40\u0e15\u0e2d\u0e23\u0e4c\u0e17\u0e32\u0e07\u0e01\u0e32\u0e23\u0e2a\u0e42\u0e21\u0e2a\u0e23\u0e25\u0e34\u0e40\u0e27\u0e2d\u0e23\u0e4c\u0e1e\u0e39\u0e25\u0e2a\u0e33\u0e2b\u0e23\u0e31\u0e1a\u0e1b\u0e23\u0e30\u0e40\u0e17\u0e28\u0e44\u0e17\u0e22 \u0e1f\u0e2d\u0e25\u0e42\u0e25\u0e27\u0e4c @ThaiLFC \u0e40\u0e1e\u0e37\u0e48\u0e2d\u0e23\u0e48\u0e27\u0e21\u0e40\u0e1b\u0e47\u0e19\u0e2b\u0e19\u0e36\u0e48\u0e07\u0e43\u0e19\u0e04\u0e23\u0e2d\u0e1a\u0e04\u0e23\u0e31\u0e27\u0e2a\u0e42\u0e21\u0e2a\u0e23\u0e1f\u0e38\u0e15\u0e1a\u0e2d\u0e25\u0e17\u0e35\u0e48\u0e43\u0e2b\u0e0d\u0e48\u0e17\u0e35\u0e48\u0e2a\u0e38\u0e14\u0e43\u0e19\u0e42\u0e25\u0e01\u0e14\u0e49\u0e27\u0e22\u0e01\u0e31\u0e19 #LFCThai \u0e17\u0e27\u0e34\u0e15\u0e40\u0e15\u0e2d\u0e23\u0e4c\u0e17\u0e32\u0e07\u0e01\u0e32\u0e23 @LFC","translator_type":"none","protected":false,"verified":true,"followers_count":715059,"friends_count":19407,"listed_count":288,"favourites_count":2990,"statuses_count":48914,"created_at":"Thu Oct 25 21:40:42 +0000 2012","utc_offset":null,"time_zone":null,"geo_enabled":false,"lang":"th","contributors_enabled":false,"is_translator":false,"profile_background_color":"F7F6EF","profile_background_image_url":"http:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png","profile_background_image_url_https":"https:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png","profile_background_tile":true,"profile_link_color":"0099CC","profile_sidebar_border_color":"FFFFFF","profile_sidebar_fill_color":"DDEEF6","profile_text_color":"333333","profile_use_background_image":true,"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/983381222096158720\/yZdKdQ2R_normal.jpg","profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/983381222096158720\/yZdKdQ2R_normal.jpg","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/904718268\/1525437848","default_profile":false,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},"geo":null,"coordinates":null,"place":null,"contributors":null,"is_quote_status":false,"quote_count":0,"reply_count":0,"retweet_count":108,"favorite_count":36,"entities":{"hashtags":[{"text":"WeAreLiverpool","indices":[32,47]},{"text":"UCLfinal","indices":[48,57]}],"urls":[],"user_mentions":[],"symbols":[],"media":[{"id":1000450881798660096,"id_str":"1000450881798660096","indices":[58,81],"media_url":"http:\/\/pbs.twimg.com\/media\/DeJQxsDV4AAGPBm.jpg","media_url_https":"https:\/\/pbs.twimg.com\/media\/DeJQxsDV4AAGPBm.jpg","url":"https:\/\/t.co\/e86MUogrZt","display_url":"pic.twitter.com\/e86MUogrZt","expanded_url":"https:\/\/twitter.com\/ThaiLFC\/status\/1000450896738725890\/photo\/1","type":"photo","sizes":{"thumb":{"w":150,"h":150,"resize":"crop"},"large":{"w":1200,"h":800,"resize":"fit"},"small":{"w":680,"h":453,"resize":"fit"},"medium":{"w":1200,"h":800,"resize":"fit"}}}]},"extended_entities":{"media":[{"id":1000450881798660096,"id_str":"1000450881798660096","indices":[58,81],"media_url":"http:\/\/pbs.twimg.com\/media\/DeJQxsDV4AAGPBm.jpg","media_url_https":"https:\/\/pbs.twimg.com\/media\/DeJQxsDV4AAGPBm.jpg","url":"https:\/\/t.co\/e86MUogrZt","display_url":"pic.twitter.com\/e86MUogrZt","expanded_url":"https:\/\/twitter.com\/ThaiLFC\/status\/1000450896738725890\/photo\/1","type":"photo","sizes":{"thumb":{"w":150,"h":150,"resize":"crop"},"large":{"w":1200,"h":800,"resize":"fit"},"small":{"w":680,"h":453,"resize":"fit"},"medium":{"w":1200,"h":800,"resize":"fit"}}}]},"favorited":false,"retweeted":false,"possibly_sensitive":false,"filter_level":"low","lang":"th"},"is_quote_status":false,"quote_count":0,"reply_count":0,"retweet_count":0,"favorite_count":0,"entities":{"hashtags":[{"text":"WeAreLiverpool","indices":[45,60]},{"text":"UCLfinal","indices":[61,70]}],"urls":[],"user_mentions":[{"screen_name":"ThaiLFC","name":"LFC Thailand","id":904718268,"id_str":"904718268","indices":[3,11]}],"symbols":[],"media":[{"id":1000450881798660096,"id_str":"1000450881798660096","indices":[71,94],"media_url":"http:\/\/pbs.twimg.com\/media\/DeJQxsDV4AAGPBm.jpg","media_url_https":"https:\/\/pbs.twimg.com\/media\/DeJQxsDV4AAGPBm.jpg","url":"https:\/\/t.co\/e86MUogrZt","display_url":"pic.twitter.com\/e86MUogrZt","expanded_url":"https:\/\/twitter.com\/ThaiLFC\/status\/1000450896738725890\/photo\/1","type":"photo","sizes":{"thumb":{"w":150,"h":150,"resize":"crop"},"large":{"w":1200,"h":800,"resize":"fit"},"small":{"w":680,"h":453,"resize":"fit"},"medium":{"w":1200,"h":800,"resize":"fit"}},"source_status_id":1000450896738725890,"source_status_id_str":"1000450896738725890","source_user_id":904718268,"source_user_id_str":"904718268"}]},"extended_entities":{"media":[{"id":1000450881798660096,"id_str":"1000450881798660096","indices":[71,94],"media_url":"http:\/\/pbs.twimg.com\/media\/DeJQxsDV4AAGPBm.jpg","media_url_https":"https:\/\/pbs.twimg.com\/media\/DeJQxsDV4AAGPBm.jpg","url":"https:\/\/t.co\/e86MUogrZt","display_url":"pic.twitter.com\/e86MUogrZt","expanded_url":"https:\/\/twitter.com\/ThaiLFC\/status\/1000450896738725890\/photo\/1","type":"photo","sizes":{"thumb":{"w":150,"h":150,"resize":"crop"},"large":{"w":1200,"h":800,"resize":"fit"},"small":{"w":680,"h":453,"resize":"fit"},"medium":{"w":1200,"h":800,"resize":"fit"}},"source_status_id":1000450896738725890,"source_status_id_str":"1000450896738725890","source_user_id":904718268,"source_user_id_str":"904718268"}]},"favorited":false,"retweeted":false,"possibly_sensitive":false,"filter_level":"low","lang":"th","timestamp_ms":"1527361253680"}"#;
        let json_value = json::parse(json).unwrap();
        let schema = infer_schema(&json_value, "myschema").unwrap();
//        assert!(&schema.is_ok());
        println!("{}", schema.canonical_form());
    }

    #[test]
    fn test_infer_schema_performance() {
        let now = Instant::now();
        let mut schemas =
            GzipFile::new("/usr/local/google/home/shafirasulov/IdeaProjects/learningrust/TweetsChampions.json.gz")
                .lines
                .take(5000)
//                .map(|line| serde_json::from_str(line.unwrap().as_str()).unwrap())
                .map(|line| json::parse(line.unwrap().as_str()).unwrap())
                .enumerate()
                .map(|(i, line)| infer_schema(&line, "inferred_schema"))
//                .for_each(|(i, line)| {
//                    let s = infer_schema(&line, "inferred_schema");
//                })
            ;

        let first = schemas.next().unwrap();
        schemas
            .fold(first, |base, next| {
                let f = merge_schemas(base.unwrap(), next.unwrap());
                f
            });

        println!("Elapsed: {}", now.elapsed().as_millis());
    }

    #[test]
    fn test_merge_schema() {
        let schema1 = Schema::parse_str(r#"{"name":"variants","type":"record","fields":[{"name":"bitrate","type":["null","long"]},{"name":"url","type":"string"},{"name":"content_type","type":"long"}]}"#).unwrap();

        let schema2 = Schema::parse_str(r#"{"name":"variants","type":"record","fields":[{"name":"bitrate","type":"long"},{"name":"content_type","type":"long"},{"name":"url","type":"string"}]}"#).unwrap();

        let merged_schema = merge_schemas(schema1.clone(), schema2);
        println!("{:?}", &merged_schema.unwrap().canonical_form());
    }

//    #[test]
//    fn test_json_to_avro() {
//        let txt = r#"{"a": 1, "b": 2, "c": [1, "alma", true]}"#;
//        let json = json::parse(txt).unwrap();
//        let avro = json_to_avro(json).unwrap();
//        println!("{:?}", avro);
//    }

    fn test_file(n_rows: usize) -> impl Iterator<Item=String> {
        GzipFile::new("/usr/local/google/home/shafirasulov/IdeaProjects/learningrust/TweetsChampions.json.gz")
            .lines
            .map(|l| l.unwrap())
            .take(n_rows)
    }

//    #[test]
//    fn test_end_to_end() {
//        let now = Instant::now();
//
//        let mut schemas =
//            test_file(50000000)
//                .map(|line| json::parse(line.as_str()).unwrap())
//                .enumerate()
//                .map(|(i, line)| infer_schema(&line, "inferred_schema"))
//            ;
//
//        let first = schemas.next().unwrap();
//        let final_schema = schemas
//            .fold(first, |base, next| {
//                let f = merge_schemas(base.unwrap(), next.unwrap());
//                f
//            }).unwrap();
//
//        let mut file = File::create("test.avro").unwrap();
//        let mut writer = Writer::with_codec(&final_schema, file, Codec::Deflate);
//        for line in test_file(5000000) {
//            let json = json::parse(&line).unwrap();
//            let avro = json_to_avro(json).unwrap();
//            writer.append(avro).unwrap();
//        }
//
//        writer.flush();
//        println!("Elapsed: {}", now.elapsed().as_millis());
//    }
}