use std::env;
use std::path::PathBuf;

use fastdup_testkit::generate_structured_corpus;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args_os().skip(1);
    let root = arguments.next().map_or_else(
        || PathBuf::from(".artifacts/corpus/structured"),
        PathBuf::from,
    );
    if arguments.next().is_some() {
        return Err("usage: generate_structured_corpus [output-directory]".into());
    }
    generate_structured_corpus(&root)?;
    println!("structured_corpus={}", root.display());
    Ok(())
}
