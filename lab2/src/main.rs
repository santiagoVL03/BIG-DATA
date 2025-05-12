use rayon::prelude::*;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
    sync::{Arc, Mutex},
    time::Instant,
};

use crossbeam_channel::unbounded;
const BATCH_SIZE: usize = 10 * 1024 * 1024; // 10 MB
fn main() -> std::io::Result<()> {
    let palabras_clave = vec!["error", "warning", "failed"];
    let palabras_clave = Arc::new(palabras_clave);

    let file_paths: Vec<_> = (1..=18)
        .map(|i| format!("./utils/n_{}_archivo_1gb.txt", i))
        .collect();

    let resultados: Arc<Mutex<HashMap<String, usize>>> = Arc::new(Mutex::new(HashMap::new()));

    let start = Instant::now();

    file_paths.par_iter().for_each(|path| {
        let mut file = File::open(path).expect("No se pudo abrir el archivo");
        let mut offset = 0u64;

        loop {
            let mut buffer = vec![0u8; BATCH_SIZE];
            file.seek(SeekFrom::Start(offset)).unwrap();

            let bytes_leidos = file.read(&mut buffer).unwrap();
            if bytes_leidos == 0 {
                break;
            }

            let contenido = String::from_utf8_lossy(&buffer[..bytes_leidos]);

            let mut local_map = HashMap::new();
            for palabra in palabras_clave.iter() {
                let count = contenido.matches(palabra).count();
                if count > 0 {
                    local_map.insert(palabra.to_string(), count);
                }
            }

            // Merge resultados
            let mut global_map = resultados.lock().unwrap();
            for (k, v) in local_map {
                *global_map.entry(format!("{} en {}", k, path)).or_insert(0) += v;
            }

            offset += bytes_leidos as u64;
        }
    });

    let duracion = start.elapsed();
    println!("Tiempo total: {:?}", duracion);

    println!("Resultados:");
    let resultados = resultados.lock().unwrap();
    for (k, v) in resultados.iter() {
        println!("{}: {}", k, v);
    }

    Ok(())
}