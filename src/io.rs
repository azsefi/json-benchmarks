use std::fs::File;
use flate2::read::GzDecoder;
use std::io::{BufReader, BufRead, Lines};

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