use rayon::prelude::*;
use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    sync::{Arc, Mutex},
    time::Instant
};

const BATCH_SIZE: usize = 10 * 1024 * 1024; // 10 MB
fn main() -> std::io::Result<()> {
    let palabras_clave = vec!["templadero", "barcal", "taradez"];
    let palabras_clave = Arc::new(palabras_clave);

    let file_paths: Vec<_> = (0..=17)
        .map(|i| format!("../data/n_{}_archivo_1gb.txt", i))
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

    let mut output_file = File::create("resultados.txt")?;
    writeln!(output_file, "Tiempo total: {:?}", duracion)?;
    writeln!(output_file, "Resultados:")?;
    for (k, v) in resultados.iter() {
        writeln!(output_file, "{}: {}", k, v)?;
    }
    println!("Resultados guardados en 'resultados.txt'");
    Ok(())
}