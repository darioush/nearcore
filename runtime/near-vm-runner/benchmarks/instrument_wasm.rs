use near_parameters::RuntimeConfigStore;
use near_parameters::vm::VMKind;
use near_primitives_core::version::PROTOCOL_VERSION;
use near_vm_runner::prepare;
use std::path::PathBuf;

fn main() {
    let mut args = std::env::args().skip(1);
    let input = args.next().unwrap_or_else(|| {
        eprintln!("usage: bench-instrument-wasm <input.wasm> [output.wasm]");
        eprintln!("  default output: <input>.instrumented.wasm");
        std::process::exit(1);
    });
    let output = args.next().map(PathBuf::from).unwrap_or_else(|| {
        let mut p = PathBuf::from(&input);
        let stem = p.file_stem().expect("input must have a name").to_owned();
        let mut name = stem;
        name.push(".instrumented.wasm");
        p.set_file_name(name);
        p
    });

    let wasm = std::fs::read(&input).expect("input wasm should exist");

    let store = RuntimeConfigStore::new(None);
    let runtime_config = store.get_config(PROTOCOL_VERSION);
    let mut wasm_config = near_parameters::vm::Config::clone(&runtime_config.wasm_config);
    wasm_config.vm_kind = VMKind::Wasmtime;

    let prepared = prepare::prepare_contract(&wasm, &wasm_config, VMKind::Wasmtime)
        .expect("prepare_contract failed");

    std::fs::write(&output, &prepared).expect("write instrumented wasm");
    eprintln!(
        "wrote {} ({} bytes -> {} bytes)",
        output.display(),
        wasm.len(),
        prepared.len()
    );
}
