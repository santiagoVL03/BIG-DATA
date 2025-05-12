use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    sync::{Arc, Mutex},
    thread,
    time::Instant,
};

use crossbeam_channel::unbounded;

const BATCH_SIZE: usize = 10 * 1024 * 1024; // 10 MB

fn main() -> std::io::Result<()> {
    let path = "./utils/archivo_18gb.txt";
    let file = File::open(path)?;
    let file_size = file.metadata()?.len();

    let num_threads = num_cpus::get();
    let file = Arc::new(Mutex::new(file));
    let (sender, receiver) = unbounded();

    let start = Instant::now();
    for i in 0..num_threads {
        let file = Arc::clone(&file);
        let sender = sender.clone();

        thread::spawn(move || {
            let thread_start_time = Instant::now();
            let mut local_word_count = 0;
            let mut local_map: HashMap<String, usize> = HashMap::new();
            let mut offset = i as u64 * BATCH_SIZE as u64;

            while offset < file_size {
                let mut buffer = vec![0u8; BATCH_SIZE];
                let mut guard = file.lock().unwrap();
                guard.seek(SeekFrom::Start(offset)).unwrap();
                let bytes_read = guard.read(&mut buffer).unwrap();
                drop(guard);

                if bytes_read == 0 {
                    break;
                }

                if bytes_read == BATCH_SIZE {
                    if let Some(pos) = buffer.iter().rposition(|&b| b == b' ' || b == b'\n') {
                        buffer.truncate(pos + 1);
                    }
                }

                let text = String::from_utf8_lossy(&buffer);
                for word in text
                    .split_whitespace()
                    .map(|w| w.trim_matches(|c: char| !c.is_alphanumeric()))
                    .filter(|w| !w.is_empty())
                {
                    let word = word.to_lowercase();
                    *local_map.entry(word).or_insert(0) += 1;
                    local_word_count += 1;
                }

                offset += BATCH_SIZE as u64 * num_threads as u64;
            }
            let thread_duration = thread_start_time.elapsed();
            println!(
                "Thread {} finished in {:?} with {} words",
                i, thread_duration, local_word_count
            );
            sender.send((local_word_count, local_map)).unwrap();
        });
    }

    drop(sender);

    let mut global_map: HashMap<String, usize> = HashMap::new();
    let mut total_words = 0;

    for (count, map) in receiver.iter() {
        total_words += count;
        for (word, freq) in map {
            *global_map.entry(word).or_insert(0) += freq;
        }
    }

    let duration = start.elapsed();
    println!("Tiempo total: {:?}", duration);

    let mut total_file = File::create("total_palabras.txt")?;
    writeln!(total_file, "{}", total_words)?;

    let mut top: Vec<_> = global_map.into_iter().collect();
    top.sort_unstable_by(|a, b| b.1.cmp(&a.1));

    let mut top_file = File::create("top__palabras.txt")?;
    for (word, count) in top.into_iter() {
        writeln!(top_file, "{}: {}", word, count)?;
    }

    println!("Total de palabras: {}", total_words);
    Ok(())
}