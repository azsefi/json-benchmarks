use json;
use std::collections::{HashMap, HashSet};
use json::JsonValue;
use json::number::Number;
use avro_rs::Schema;
use avro_rs::schema::{Name, UnionSchema, RecordField, RecordFieldOrder, SchemaKind};
use serde_json;
use serde_json::{Value, Map};
use failure::Error;
use std::fs::File;
use flate2::read::GzDecoder;
use std::io::{BufReader, BufRead, Lines};
use regex::Regex;
use std::borrow::Cow;
use std::ops::{DerefMut, Deref};


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

fn infer_schema(json_value: &JsonValue, name: &str) -> Result<Schema, Error> {
    match json_value {
        JsonValue::Boolean(_) => Ok(Schema::Boolean),
        JsonValue::String(_) => Ok(Schema::String),
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
                    Ok(Schema::Null)
                };

            Ok(Schema::Array(Box::new(items_schema?)))
        },
        JsonValue::Object(_) => {
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
        JsonValue::Null => Ok(Schema::Null),
        JsonValue::Short(_) => Ok(Schema::Long),
        _ => Ok(Schema::Null)
    }
}


pub fn merge_schemas(schema1: Schema, schema2: Schema) -> Result<Schema, Error> {
    match (schema1, schema2) {
        (Schema::Record {name: name1, doc: doc1, fields: mut fields1, lookup: lookup1},
            Schema::Record {name: name2, doc: doc2, fields: mut fields2, lookup: mut lookup2}) => {
            let mut all_fields = HashMap::new();
            for field in fields1 {
                all_fields.insert((&field).name.clone(), vec![field]);
            }

            for field in fields2 {
                let key = (&field).name.as_str();
                if let Some(v) = all_fields.get_mut(key) {
                    v.push(field);
                } else {
                    all_fields.insert(key.to_owned(), vec![field]);
                }
            }

            let mut merged_fields = Vec::new();
            for (field_name, mut fields) in all_fields {
                if fields.len() == 1 {
                    let mut field = fields.pop().unwrap();
                    let schema = merge_schemas(Schema::Null, field.schema)?;
//                    let schema = Schema::Union(UnionSchema::new(vec![Schema::Null, field.schema])?);
                    field.position = merged_fields.len();
                    field.schema = schema;
                    field.default = Some(Value::Null);
                    merged_fields.push(field);
                } else {
                    let (mut field1, field2) = (fields.pop().unwrap(), fields.pop().unwrap());
                    let merged_schema = merge_schemas(field1.schema, field2.schema);
                    field1.schema = merged_schema?;
                    field1.position = merged_fields.len();
                    merged_fields.push(field1);
                }
            }

            let lookup: HashMap<String, usize> = merged_fields.iter().enumerate().map(|(i, field)| (field.name.clone(), i)).collect();
            Ok(Schema::Record {name: name1, doc: doc1, fields: merged_fields, lookup})
        }
        (Schema::Union(mut us1), Schema::Union(mut us2)) => {
            let mut schema_kinds: HashMap<SchemaKind, Vec<Schema>> = HashMap::new();

            while let Some(schema) = us1.variants_mut().pop() {
                let sk = SchemaKind::from(&schema);
                schema_kinds.insert(sk, vec![schema]);
            }

            while let Some(schema) = us2.variants_mut().pop() {
                let sk = SchemaKind::from(&schema);
                if let Some(v) = schema_kinds.get_mut(&sk) {
                    v.push(schema);
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
                    merged_schemas.push(merge_schemas(schemas.pop().unwrap(), schemas.pop().unwrap())?);
                }
            }

            Ok(Schema::Union(UnionSchema::new(merged_schemas)?))
        }
        (Schema::Union(mut us1), s2 ) => {
            let mut schema_kinds = HashMap::new();
            while let Some(schema) = us1.variants_mut().pop() {
                let sk = SchemaKind::from(&schema);
                schema_kinds.insert(sk, vec![schema]);
            }

            let sk = SchemaKind::from(&s2);
            if let Some(v) = schema_kinds.get_mut(&sk) {
                v.push(s2);
            } else {
                schema_kinds.insert(sk, vec![s2]);
            }

            let mut merged_schemas = Vec::new();
            let sk = SchemaKind::from(&Schema::Null);
            if schema_kinds.remove(&sk).is_some() {
                merged_schemas.push(Schema::Null);
            }
            for (_, mut schemas) in schema_kinds {
                if schemas.len() == 1 {
                    merged_schemas.push(schemas.pop().unwrap());
                } else {
                    merged_schemas.push(merge_schemas(schemas.pop().unwrap(), schemas.pop().unwrap())?);
                }
            }

            Ok(Schema::Union(UnionSchema::new(merged_schemas)?))
        }
        (s2, Schema::Union(mut us1) ) => {
            let mut schema_kinds = HashMap::new();
            while let Some(schema) = us1.variants_mut().pop() {
                let sk = SchemaKind::from(&schema);
                schema_kinds.insert(sk, vec![schema]);
            }

            let sk = SchemaKind::from(&s2);
            if let Some(v) = schema_kinds.get_mut(&sk) {
                v.push(s2);
            } else {
                schema_kinds.insert(sk, vec![s2]);
            }

            let mut merged_schemas = Vec::new();
            let sk = SchemaKind::from(&Schema::Null);
            if schema_kinds.remove(&sk).is_some() {
                merged_schemas.push(Schema::Null);
            }
            for (_, mut schemas) in schema_kinds {
                if schemas.len() == 1 {
                    merged_schemas.push(schemas.pop().unwrap());
                } else {
                    merged_schemas.push(merge_schemas(schemas.pop().unwrap(), schemas.pop().unwrap())?);
                }
            }

            Ok(Schema::Union(UnionSchema::new(merged_schemas)?))
        }
        (Schema::Array(mut schema1), Schema::Array(schema2)) => {
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



fn clean_name(txt: &str) -> String {
    let re = Regex::new(r"[^A-Za-z\d]").unwrap();
    let pre_clean = re.replace_all(txt, "_").to_string();

    let re = Regex::new(r"__+").unwrap();
    let mut clean =
        re
            .replace_all(pre_clean.as_str(), "_")
            .trim_start_matches("_")
            .trim_end_matches("_")
            .to_string();

    if let Some(c) = clean.chars().next() {
        if c.is_numeric() {
            clean = "_".to_owned() + clean.as_str()
        }
    }
    clean
}


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

//    #[test]
//    fn test_clean_name() {
//        let name = "**1abc*(de&&";
//        let clean = clean_name(name);
//        assert_eq!(clean, "_1abc_de")
//    }

//    #[test]
//    fn test_infer_schema() {
//        let json = r#"{"created_at":"Sat May 26 19:00:53 +0000 2018","id":1000451726212714496,"id_str":"1000451726212714496","text":"RT @ThaiLFC: \ud83d\ude4c \u0e40\u0e23\u0e32\u0e04\u0e37\u0e2d\u0e25\u0e34\u0e40\u0e27\u0e2d\u0e23\u0e4c\u0e1e\u0e39\u0e25\u0e25\u0e25\u0e25\u0e25\u0e25\u0e25!!!!!!\n\n#WeAreLiverpool #UCLfinal https:\/\/t.co\/e86MUogrZt","source":"\u003ca href=\"http:\/\/twitter.com\/download\/android\" rel=\"nofollow\"\u003eTwitter for Android\u003c\/a\u003e","truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user":{"id":399193055,"id_str":"399193055","name":"\u25cbSP \u30df\u30f3\ud83c\udf3b\u25cb","screen_name":"speirmint2828","location":null,"url":null,"description":"don't wake me, I'm not dreaming. \ud83c\udf43\ud83c\udf42 \u0e44\u0e21\u0e48\u0e15\u0e49\u0e2d\u0e07\u0e2a\u0e32\u0e23\u0e30\u0e41\u0e19\u0e41\u0e04\u0e1b\u0e04\u0e48\u0e30 \u0e40\u0e21\u0e19\u0e0a\u0e31\u0e48\u0e19\u0e21\u0e32","translator_type":"regular","protected":false,"verified":false,"followers_count":1416,"friends_count":225,"listed_count":10,"favourites_count":7462,"statuses_count":220056,"created_at":"Thu Oct 27 03:59:17 +0000 2011","utc_offset":null,"time_zone":null,"geo_enabled":false,"lang":"th","contributors_enabled":false,"is_translator":false,"profile_background_color":"ACDED6","profile_background_image_url":"http:\/\/abs.twimg.com\/images\/themes\/theme14\/bg.gif","profile_background_image_url_https":"https:\/\/abs.twimg.com\/images\/themes\/theme14\/bg.gif","profile_background_tile":true,"profile_link_color":"048504","profile_sidebar_border_color":"FFFFFF","profile_sidebar_fill_color":"EFEFEF","profile_text_color":"333333","profile_use_background_image":true,"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/998156737818411008\/2Zb3OxEF_normal.jpg","profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/998156737818411008\/2Zb3OxEF_normal.jpg","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/399193055\/1522692044","default_profile":false,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},"geo":null,"coordinates":null,"place":null,"contributors":null,"retweeted_status":{"created_at":"Sat May 26 18:57:35 +0000 2018","id":1000450896738725890,"id_str":"1000450896738725890","text":"\ud83d\ude4c \u0e40\u0e23\u0e32\u0e04\u0e37\u0e2d\u0e25\u0e34\u0e40\u0e27\u0e2d\u0e23\u0e4c\u0e1e\u0e39\u0e25\u0e25\u0e25\u0e25\u0e25\u0e25\u0e25!!!!!!\n\n#WeAreLiverpool #UCLfinal https:\/\/t.co\/e86MUogrZt","display_text_range":[0,57],"source":"\u003ca href=\"http:\/\/twitter.com\/download\/iphone\" rel=\"nofollow\"\u003eTwitter for iPhone\u003c\/a\u003e","truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user":{"id":904718268,"id_str":"904718268","name":"LFC Thailand","screen_name":"ThaiLFC","location":"Bangkok","url":"http:\/\/thailand.liverpoolfc.com\/","description":"\u0e17\u0e27\u0e34\u0e15\u0e40\u0e15\u0e2d\u0e23\u0e4c\u0e17\u0e32\u0e07\u0e01\u0e32\u0e23\u0e2a\u0e42\u0e21\u0e2a\u0e23\u0e25\u0e34\u0e40\u0e27\u0e2d\u0e23\u0e4c\u0e1e\u0e39\u0e25\u0e2a\u0e33\u0e2b\u0e23\u0e31\u0e1a\u0e1b\u0e23\u0e30\u0e40\u0e17\u0e28\u0e44\u0e17\u0e22 \u0e1f\u0e2d\u0e25\u0e42\u0e25\u0e27\u0e4c @ThaiLFC \u0e40\u0e1e\u0e37\u0e48\u0e2d\u0e23\u0e48\u0e27\u0e21\u0e40\u0e1b\u0e47\u0e19\u0e2b\u0e19\u0e36\u0e48\u0e07\u0e43\u0e19\u0e04\u0e23\u0e2d\u0e1a\u0e04\u0e23\u0e31\u0e27\u0e2a\u0e42\u0e21\u0e2a\u0e23\u0e1f\u0e38\u0e15\u0e1a\u0e2d\u0e25\u0e17\u0e35\u0e48\u0e43\u0e2b\u0e0d\u0e48\u0e17\u0e35\u0e48\u0e2a\u0e38\u0e14\u0e43\u0e19\u0e42\u0e25\u0e01\u0e14\u0e49\u0e27\u0e22\u0e01\u0e31\u0e19 #LFCThai \u0e17\u0e27\u0e34\u0e15\u0e40\u0e15\u0e2d\u0e23\u0e4c\u0e17\u0e32\u0e07\u0e01\u0e32\u0e23 @LFC","translator_type":"none","protected":false,"verified":true,"followers_count":715059,"friends_count":19407,"listed_count":288,"favourites_count":2990,"statuses_count":48914,"created_at":"Thu Oct 25 21:40:42 +0000 2012","utc_offset":null,"time_zone":null,"geo_enabled":false,"lang":"th","contributors_enabled":false,"is_translator":false,"profile_background_color":"F7F6EF","profile_background_image_url":"http:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png","profile_background_image_url_https":"https:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png","profile_background_tile":true,"profile_link_color":"0099CC","profile_sidebar_border_color":"FFFFFF","profile_sidebar_fill_color":"DDEEF6","profile_text_color":"333333","profile_use_background_image":true,"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/983381222096158720\/yZdKdQ2R_normal.jpg","profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/983381222096158720\/yZdKdQ2R_normal.jpg","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/904718268\/1525437848","default_profile":false,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},"geo":null,"coordinates":null,"place":null,"contributors":null,"is_quote_status":false,"quote_count":0,"reply_count":0,"retweet_count":108,"favorite_count":36,"entities":{"hashtags":[{"text":"WeAreLiverpool","indices":[32,47]},{"text":"UCLfinal","indices":[48,57]}],"urls":[],"user_mentions":[],"symbols":[],"media":[{"id":1000450881798660096,"id_str":"1000450881798660096","indices":[58,81],"media_url":"http:\/\/pbs.twimg.com\/media\/DeJQxsDV4AAGPBm.jpg","media_url_https":"https:\/\/pbs.twimg.com\/media\/DeJQxsDV4AAGPBm.jpg","url":"https:\/\/t.co\/e86MUogrZt","display_url":"pic.twitter.com\/e86MUogrZt","expanded_url":"https:\/\/twitter.com\/ThaiLFC\/status\/1000450896738725890\/photo\/1","type":"photo","sizes":{"thumb":{"w":150,"h":150,"resize":"crop"},"large":{"w":1200,"h":800,"resize":"fit"},"small":{"w":680,"h":453,"resize":"fit"},"medium":{"w":1200,"h":800,"resize":"fit"}}}]},"extended_entities":{"media":[{"id":1000450881798660096,"id_str":"1000450881798660096","indices":[58,81],"media_url":"http:\/\/pbs.twimg.com\/media\/DeJQxsDV4AAGPBm.jpg","media_url_https":"https:\/\/pbs.twimg.com\/media\/DeJQxsDV4AAGPBm.jpg","url":"https:\/\/t.co\/e86MUogrZt","display_url":"pic.twitter.com\/e86MUogrZt","expanded_url":"https:\/\/twitter.com\/ThaiLFC\/status\/1000450896738725890\/photo\/1","type":"photo","sizes":{"thumb":{"w":150,"h":150,"resize":"crop"},"large":{"w":1200,"h":800,"resize":"fit"},"small":{"w":680,"h":453,"resize":"fit"},"medium":{"w":1200,"h":800,"resize":"fit"}}}]},"favorited":false,"retweeted":false,"possibly_sensitive":false,"filter_level":"low","lang":"th"},"is_quote_status":false,"quote_count":0,"reply_count":0,"retweet_count":0,"favorite_count":0,"entities":{"hashtags":[{"text":"WeAreLiverpool","indices":[45,60]},{"text":"UCLfinal","indices":[61,70]}],"urls":[],"user_mentions":[{"screen_name":"ThaiLFC","name":"LFC Thailand","id":904718268,"id_str":"904718268","indices":[3,11]}],"symbols":[],"media":[{"id":1000450881798660096,"id_str":"1000450881798660096","indices":[71,94],"media_url":"http:\/\/pbs.twimg.com\/media\/DeJQxsDV4AAGPBm.jpg","media_url_https":"https:\/\/pbs.twimg.com\/media\/DeJQxsDV4AAGPBm.jpg","url":"https:\/\/t.co\/e86MUogrZt","display_url":"pic.twitter.com\/e86MUogrZt","expanded_url":"https:\/\/twitter.com\/ThaiLFC\/status\/1000450896738725890\/photo\/1","type":"photo","sizes":{"thumb":{"w":150,"h":150,"resize":"crop"},"large":{"w":1200,"h":800,"resize":"fit"},"small":{"w":680,"h":453,"resize":"fit"},"medium":{"w":1200,"h":800,"resize":"fit"}},"source_status_id":1000450896738725890,"source_status_id_str":"1000450896738725890","source_user_id":904718268,"source_user_id_str":"904718268"}]},"extended_entities":{"media":[{"id":1000450881798660096,"id_str":"1000450881798660096","indices":[71,94],"media_url":"http:\/\/pbs.twimg.com\/media\/DeJQxsDV4AAGPBm.jpg","media_url_https":"https:\/\/pbs.twimg.com\/media\/DeJQxsDV4AAGPBm.jpg","url":"https:\/\/t.co\/e86MUogrZt","display_url":"pic.twitter.com\/e86MUogrZt","expanded_url":"https:\/\/twitter.com\/ThaiLFC\/status\/1000450896738725890\/photo\/1","type":"photo","sizes":{"thumb":{"w":150,"h":150,"resize":"crop"},"large":{"w":1200,"h":800,"resize":"fit"},"small":{"w":680,"h":453,"resize":"fit"},"medium":{"w":1200,"h":800,"resize":"fit"}},"source_status_id":1000450896738725890,"source_status_id_str":"1000450896738725890","source_user_id":904718268,"source_user_id_str":"904718268"}]},"favorited":false,"retweeted":false,"possibly_sensitive":false,"filter_level":"low","lang":"th","timestamp_ms":"1527361253680"}"#;
//        let json_value = json::parse(json).unwrap();
//        let schema = infer_schema(&json_value, "myschema");
////        assert!(&schema.is_ok());
//        println!("{}", schema.canonical_form());
//    }

    #[test]
    fn test_infer_schema_performance() {
        let now = Instant::now();
        let mut schemas =
        GzipFile::new("/usr/local/google/home/shafirasulov/IdeaProjects/learningrust/TweetsChampions.json.gz")
            .lines
//            .take(5000)
            .map(|line| json::parse(line.unwrap().as_str()).unwrap())
            .enumerate()
            .map(|(i, line)| infer_schema(&line, "inferred_schema"));

        let first = schemas.next().unwrap();
        schemas
            .fold(first, |base, next| {
                let f = merge_schemas(base.unwrap(), next.unwrap());
                f
            });

        println!("Elapsed: {}", now.elapsed().as_millis());
    }

//    #[test]
//    fn test_merge_schema() {
//        let schema1 = Schema::parse_str(r#"{"name":"variants","type":"record","fields":[{"name":"bitrate","type":["null","long"]},{"name":"url","type":"string"},{"name":"content_type","type":"long"}]}"#).unwrap();
//
//        let schema2 = Schema::parse_str(r#"{"name":"variants","type":"record","fields":[{"name":"bitrate","type":"long"},{"name":"content_type","type":"long"},{"name":"url","type":"string"}]}"#).unwrap();
//
//        let merged_schema = merge_schemas(schema1.clone(), schema2);
//        println!("{:?}", &merged_schema.unwrap().canonical_form());
//    }
}