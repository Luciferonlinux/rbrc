use clap::Parser;
#[cfg(debug_assertions)]
use pretty_env_logger::env_logger::WriteStyle;
use rbrc_calc::*;
use std::{collections::HashMap, fs::File};

#[derive(Debug, Parser, Clone)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, required = false, default_value_t = 1)]
    jobs: usize,
    #[arg(required = true, value_name = "FILE")]
    filename: String,
}

fn main() {
    #[cfg(debug_assertions)]
    pretty_env_logger::formatted_builder()
        .format_timestamp(None)
        .filter(None, log::LevelFilter::Info)
        .write_style(WriteStyle::Always)
        .target(pretty_env_logger::env_logger::Target::Pipe(Box::new(
            std::fs::File::create("logs/main.log")
                .expect("failed to create log file logs/main.log"),
        )))
        .init();

    let args = Cli::parse();

    let file = File::open(args.filename.as_str()).unwrap();
    let mmap = unsafe {
        // We leak this so rust does not have to clean up the Mmap so the process exits and then
        // the OS does the dirty work
        Box::leak(Box::new(
            memmap2::MmapOptions::new().populate().map(&file).unwrap(),
        ))
    };
    let map = &mmap[0..mmap.len() - 8];

    //let mut map = Vec::with_capacity(file.metadata().unwrap().len() as usize);
    //file.read_to_end(&mut map).unwrap();

    let num_threads = args.jobs; //std::thread::available_parallelism().unwrap().into();
    let segments = segments(map, num_threads);

    let mut db: HashMap<&str, Station> = HashMap::new();

    std::thread::scope(|s| {
        let mut thread_handles = Vec::with_capacity(segments.len());
        for (i, segment) in segments.iter().enumerate() {
            let handle = std::thread::Builder::new()
                .name(format!("Worker [{:3}]", i))
                .spawn_scoped(s, move || process_segment(map, *segment))
                .unwrap();
            thread_handles.push(handle);
        }
        for handle in thread_handles {
            let part = handle.join().unwrap();
            part.iter().for_each(|(k, v)| {
                db.entry(k)
                    .and_modify(|s| {
                        s.merge(v);
                    })
                    .or_insert(*v);
            });
        }
    });

    print!("{}", calculate_outstring(db));
}
