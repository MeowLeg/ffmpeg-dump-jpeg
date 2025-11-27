use anyhow::{Context, Result, anyhow};
use chrono::Local;
use clap::Parser;
use ffmpeg_next::{codec, format, frame, media, software::scaling};
use image::{ImageBuffer, Rgb};
use rusqlite::Connection;
use serde::Deserialize;
use std::{fs::File, io::Read, path::PathBuf, time::Instant};

mod stream;

#[derive(Debug, Parser)]
#[command(version, about, long_about=None)]
struct Cli {
    /// 处理的rtmp流地址
    #[arg(short, long)]
    url: Option<String>,

    /// config file
    #[arg(short, long, default_value = "./config.toml")]
    config: String,

    #[arg(long, default_value = "")]
    uuid: String,

    #[arg(short, long, default_value = "")]
    project_uuid: String,

    #[arg(short, long, default_value = "")]
    organization_uuid: String,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub port: u32,
    pub db_path: String,
    pub dump_path: String,
    pub predict_worker_num: u32,
    pub notify_svr_url: String,
    pub notify_timeout: u64,
    pub redis_stream_tag: String,
    pub static_dir: String,
    pub svr_root_url: String,
    pub is_test: bool,
    pub frame_interval_count: u32,
    pub watch_interval: u64,
    pub rtmp_max_timeout: u64,
    pub main_cmd: String,
    pub max_duration: f64,
}

pub fn read_from_toml(f: &str) -> Result<Config> {
    let mut file = File::open(f)?;
    let mut s = String::new();
    file.read_to_string(&mut s)?;
    let config: Config = toml::from_str(&s)?;
    Ok(config)
}

pub fn get_current_str(concat: Option<&str>) -> String {
    let now = Local::now();
    let fmt = match concat {
        Some(c) => &format!("%Y{c}%m{c}%d{c}%H{c}%M{c}%S"),
        None => "%Y-%m-%d %H:%M:%S",
    };
    now.format(fmt).to_string()
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = read_from_toml(&cli.config)?;

    if let Some(url) = &cli.url {
        let stream_md5_val = format!("{:x}", md5::compute(url.as_bytes()));
        let _ = stream::stream(
            &cfg,
            url,
            &stream_md5_val,
            &cli.uuid,
            &cli.project_uuid,
            &cli.organization_uuid,
        );
    }

    Ok(())
}
