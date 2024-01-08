use std::collections::HashMap;
use std::env;
use std::str::from_utf8_unchecked;
use tokio::io::{AsyncBufReadExt, AsyncReadExt};
use tokio::{fs::File, io::BufReader};

const BUF_CAPACITY: usize = 1024 * 1024 * 1;

struct Station {
    min: f64,
    max: f64,
    sum: f64,
    count: usize,
}

fn parse_int(bytes: &[u8]) -> usize {
    let mut result = 0;

    for byte in bytes {
        result = result * 10 + (byte - b'0') as usize;
    }

    result
}

async fn process_task(task: Vec<u8>) -> HashMap<String, Station> {
    let chars_global = task.as_slice();

    let mut idx_global = 0;

    let mut stations: HashMap<String, Station> = HashMap::new();

    loop {
        let chars = &chars_global[idx_global..];

        let mut semicolon_idx = 0;
        for char in chars {
            if *char == b';' {
                break;
            }

            semicolon_idx += 1;
        }

        if semicolon_idx == chars.len() {
            break;
        }

        let station_name = &chars[..semicolon_idx];

        let mut newline_idx = 0;
        while chars[newline_idx] != b'\n' {
            newline_idx += 1;
        }

        let mut dot_idx = 0;
        while chars[semicolon_idx..][dot_idx] != b'.' {
            dot_idx += 1;
        }

        let negative = chars[semicolon_idx + 1] == b'-';

        let num_str = if negative {
            &chars[semicolon_idx + 2..semicolon_idx + dot_idx]
        } else {
            &chars[semicolon_idx + 1..semicolon_idx + dot_idx]
        };

        let num_str_dec = &chars[semicolon_idx + dot_idx + 1..newline_idx];

        let temperature = if negative {
            -(parse_int(num_str) as f64 + parse_int(num_str_dec) as f64 / 10.0)
        } else {
            parse_int(num_str) as f64 + parse_int(num_str_dec) as f64 / 10.0
        };

        idx_global += newline_idx + 1;

        let station_name = unsafe { from_utf8_unchecked(station_name) };

        let station = stations.get_mut(station_name);

        match station {
            Some(station) => {
                station.min = station.min.min(temperature);
                station.max = station.max.max(temperature);
                station.sum += temperature;
                station.count += 1;
            }
            None => {
                let station = Station {
                    min: temperature,
                    max: temperature,
                    sum: temperature,
                    count: 1,
                };

                stations.insert(station_name.to_string(), station);
            }
        }
    }

    stations
}

#[tokio::main]
async fn main() {
    let file_path = env::args().nth(1).unwrap();

    let file = File::open(file_path).await.unwrap();
    let mut buf_read = BufReader::with_capacity(BUF_CAPACITY, file);

    let mut tasks = Vec::new();

    let mut stations: HashMap<String, Station> = HashMap::new();

    loop {
        let mut task_chunk = vec![b'\n'; BUF_CAPACITY];

        let nread = buf_read.read(task_chunk.as_mut_slice()).await.unwrap();

        if nread == 0 {
            break;
        }

        task_chunk.truncate(nread);

        buf_read.read_until(b'\n', &mut task_chunk).await.unwrap();

        task_chunk.push(b'\n');

        tasks.push(tokio::spawn(process_task(task_chunk)));
    }

    for task in tasks {
        let stations_chunk = task.await.unwrap();

        for (station_name, station_inner) in stations_chunk {
            let station_main = stations.get_mut(&station_name);

            match station_main {
                Some(station) => {
                    station.min = station.min.min(station_inner.min);
                    station.max = station.max.max(station_inner.max);
                    station.sum += station_inner.sum;
                    station.count += station_inner.count;
                }
                None => {
                    stations.insert(station_name, station_inner);
                }
            }
        }
    }

    let mut stations_sorted = stations.iter().collect::<Vec<_>>();

    stations_sorted.sort_by_key(|(id, _)| *id);

    print!("{{");

    for (id, station) in stations_sorted {
        let min = station.min;
        let max = station.max;
        let sum = station.sum;
        let count = station.count;

        print!("{}={:.1}/{:.1}/{:.1},", id, min, sum / count as f64, max,);
    }

    println!("}}");
}
