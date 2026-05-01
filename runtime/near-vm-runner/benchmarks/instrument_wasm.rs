use finite_wasm_6::max_stack::SizeConfig;
use finite_wasm_6::wasmparser as wp;
use near_parameters::RuntimeConfigStore;
use near_parameters::vm::VMKind;
use near_primitives_core::version::PROTOCOL_VERSION;
use near_vm_runner::prepare;
use prefix_sum_vec::PrefixSumVec;
use std::path::PathBuf;

struct ByteSizeCfg;
struct UnitSizeCfg;

impl SizeConfig for ByteSizeCfg {
    fn size_of_value(&self, ty: wp::ValType) -> u8 {
        use wp::ValType;
        match ty {
            ValType::I32 | ValType::F32 => 4,
            ValType::I64 | ValType::F64 => 8,
            ValType::V128 => 16,
            ValType::Ref(_) => 8,
        }
    }
    fn size_of_function_activation(&self, _: &PrefixSumVec<wp::ValType, u32>) -> u64 {
        0
    }
}

impl SizeConfig for UnitSizeCfg {
    fn size_of_value(&self, _: wp::ValType) -> u8 {
        1
    }
    fn size_of_function_activation(&self, _: &PrefixSumVec<wp::ValType, u32>) -> u64 {
        0
    }
}

fn analyze<C: SizeConfig>(wasm: &[u8], cfg: C) -> Option<Vec<u64>> {
    finite_wasm_6::Analysis::new()
        .with_stack(cfg)
        .analyze(wasm)
        .ok()
        .map(|out| out.function_operand_stack_sizes)
}

fn print_usage_and_exit() -> ! {
    eprintln!("usage: bench-instrument-wasm [--stats-only] [--header] <input.wasm> [output.wasm]");
    eprintln!("  --stats-only   skip writing instrumented wasm to disk");
    eprintln!("  --header       print CSV header to stdout and exit");
    eprintln!("  default output: <input>.instrumented.wasm");
    eprintln!();
    eprintln!("stdout: one CSV line per contract:");
    eprintln!("  hash,raw_size,instrumented_size,n_fns,");
    eprintln!("  max_operand_bytes_per_fn,sum_operand_bytes,");
    eprintln!("  max_operand_depth_per_fn,sum_operand_depth,status");
    std::process::exit(1);
}

const HEADER: &str = "hash,raw_size,instrumented_size,n_fns,\
                      max_operand_bytes_per_fn,sum_operand_bytes,\
                      max_operand_depth_per_fn,sum_operand_depth,status";

fn main() {
    let mut stats_only = false;
    let mut input: Option<String> = None;
    let mut output: Option<PathBuf> = None;
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--stats-only" => stats_only = true,
            "--header" => {
                println!("{HEADER}");
                return;
            }
            "-h" | "--help" => print_usage_and_exit(),
            _ if input.is_none() => input = Some(arg),
            _ if output.is_none() => output = Some(PathBuf::from(arg)),
            _ => print_usage_and_exit(),
        }
    }
    let Some(input) = input else { print_usage_and_exit() };

    let input_path = PathBuf::from(&input);
    let hash = input_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("?")
        .to_string();
    let raw = std::fs::read(&input_path).expect("read input");
    let raw_size = raw.len();

    let store = RuntimeConfigStore::new(None);
    let runtime_config = store.get_config(PROTOCOL_VERSION);
    let mut wasm_config = near_parameters::vm::Config::clone(&runtime_config.wasm_config);
    wasm_config.vm_kind = VMKind::Wasmtime;

    let prepare_result = prepare::prepare_contract(&raw, &wasm_config, VMKind::Wasmtime);
    let (instrumented_size, status) = match &prepare_result {
        Ok(b) => (b.len(), "OK".to_string()),
        Err(e) => (0usize, format!("REJECTED:{e:?}")),
    };

    let bytes_sizes = analyze(&raw, ByteSizeCfg);
    let depth_sizes = analyze(&raw, UnitSizeCfg);

    let n_fns = bytes_sizes.as_ref().map(|v| v.len()).unwrap_or(0);
    let max_bytes = bytes_sizes
        .as_ref()
        .and_then(|v| v.iter().copied().max())
        .unwrap_or(0);
    let sum_bytes: u64 = bytes_sizes.as_ref().map(|v| v.iter().sum()).unwrap_or(0);
    let max_depth = depth_sizes
        .as_ref()
        .and_then(|v| v.iter().copied().max())
        .unwrap_or(0);
    let sum_depth: u64 = depth_sizes.as_ref().map(|v| v.iter().sum()).unwrap_or(0);

    println!(
        "{hash},{raw_size},{instrumented_size},{n_fns},\
         {max_bytes},{sum_bytes},{max_depth},{sum_depth},{status}"
    );

    if stats_only {
        if prepare_result.is_err() {
            std::process::exit(2);
        }
        return;
    }
    match prepare_result {
        Ok(prepared) => {
            let out_path = output.unwrap_or_else(|| {
                let mut p = input_path.clone();
                let stem = p.file_stem().expect("input must have a name").to_owned();
                let mut name = stem;
                name.push(".instrumented.wasm");
                p.set_file_name(name);
                p
            });
            std::fs::write(&out_path, &prepared).expect("write instrumented wasm");
            eprintln!(
                "wrote {} ({} bytes -> {} bytes)",
                out_path.display(),
                raw_size,
                prepared.len()
            );
        }
        Err(_) => {
            // CSV line with status=REJECTED:... is already on stdout; signal failure.
            std::process::exit(2);
        }
    }
}
