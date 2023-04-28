use csv::QuoteStyle;
use polars::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::path::Path;

use std::process;

// Data structure for a mention
#[derive(Debug)]
struct Mention {
    name: String,
    party: String,
}

fn label_add_mention(file: &str, mention_file: &str, output_file: Option<&str>) -> Result<(), Box<dyn Error>> {
    // Read mention data
    let mut mentions = Vec::new();
    let mut df_mentions = CsvReader::from_path(mention_file)?
        .infer_schema(Some(16))
        .has_header(true)
        .finish()?;

    let mentions_name = df_mentions.column("name")?.utf8()?.clone();
    let mentions_party = df_mentions.column("Party")?.utf8()?.clone();

    for i in 0..mentions_name.len() {
        mentions.push(Mention {
            name: mentions_name.get(i).unwrap().to_string(),
            party: mentions_party.get(i).unwrap().to_string(),
        });
    }

    let mut df = CsvReader::from_path(file)?
        .infer_schema(Some(16))
        .has_header(true)
        .finish()?;

    let content = df.column("Content")?.utf8()?;
    let is_retweet = df.column("IsRetweet")?.bool()?;
    let rtcontent = df.column("Rtcontent")?.utf8()?;

    let mut mention_d = Vec::with_capacity(content.len());
    let mut mention_r = Vec::with_capacity(content.len());
    let mut mention_g = Vec::with_capacity(content.len());

    for i in 0..content.len() {
        let content_text = content.get(i).unwrap();
        let rtcontent_text = rtcontent.get(i).unwrap();
        let is_retweet_value = is_retweet.get(i).unwrap();

        let mut mention_flags: HashMap<String, u8> = [("D", 0), ("R", 0), ("G", 0)]
            .iter()
            .cloned()
            .collect();

        for mention in &mentions {
            let re = Regex::new(&format!(r"(?i){}", mention.name))?;
            let is_mentioned = re.is_match(content_text) || (is_retweet_value && re.is_match(rtcontent_text));

            if is_mentioned {
                *mention_flags.get_mut(&mention.party).unwrap() = 1;
            }
        }

        mention_d.push(mention_flags["D"]);
        mention_r.push(mention_flags["R"]);
        mention_g.push(mention_flags["G"]);

        println!("{}", i);
    }

    let output_path = match output_file {
        Some(path) => path,
        None => file,
    };

    df = df.with_column(
        UInt8Chunked::new_from_slice("mentionD", &mention_d).into(),
    );
    df = df.with_column(
        UInt8Chunked::new_from_slice("mentionR", &mention_r).into(),
    );
    df = df.with_column(
        UInt8Chunked::new_from_slice("mentionG", &mention_g).into(),
    );

    let file = File::create(output_path)?;
    CsvWriter::new(file)
        .has_headers(true)
        .delimiter(b',')
        .quote(b'"')
        .quote_style(QuoteStyle::Auto)
        .finish()
        .write_dataframe(&df)?;

    Ok(())
}


fn main() {
    if let Err(err) = label_add_mention("./input.csv", "./mention.csv", Some("./output.csv")) {
        eprintln!("Error: {}", err);
        process::exit(1);
    }
}
