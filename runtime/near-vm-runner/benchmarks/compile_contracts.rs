use near_parameters::RuntimeConfigStore;
use near_parameters::vm::VMKind;
use near_primitives_core::version::PROTOCOL_VERSION;
use near_vm_runner::{ContractCode, MockContractRuntimeCache, precompile_contract};
use std::sync::Arc;
use std::time::Instant;

fn main() -> anyhow::Result<()> {
    let env_filter = near_o11y::EnvFilterBuilder::from_env().verbose(Some("vm")).finish()?;
    let _subscriber = near_o11y::default_subscriber(env_filter, &Default::default()).global();

    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: bench-contracts-compilation <path>...");
        std::process::exit(1);
    }

    let vm_kind = match std::env::var("VM_KIND").as_deref() {
        Ok("NearVm") => VMKind::NearVm,
        Ok("Wasmtime") | Err(_) => VMKind::Wasmtime,
        Ok(other) => panic!("unknown VM_KIND={other}"),
    };
    let repeats: u32 = std::env::var("REPEATS").ok().and_then(|s| s.parse().ok()).unwrap_or(1);

    let store = RuntimeConfigStore::new(None);
    let config = store.get_config(PROTOCOL_VERSION);
    let mut wasm_config = near_parameters::vm::Config::clone(&config.wasm_config);
    wasm_config.vm_kind = vm_kind;
    let wasm_config = Arc::new(wasm_config);

    for path in &args {
        let wasm = std::fs::read(path)?;
        let code = ContractCode::new(wasm, None);
        let name = std::path::Path::new(path).file_name().unwrap_or_default().to_string_lossy();
        let mut min_dur = std::time::Duration::MAX;
        let mut last_status = String::from("?");
        for _ in 0..repeats {
            // fresh cache each iteration → uncached compile
            let cache = MockContractRuntimeCache::default();
            let t = Instant::now();
            let res = precompile_contract(&code, Arc::clone(&wasm_config), Some(&cache));
            let dur = t.elapsed();
            last_status = match res {
                Ok(Ok(_)) => "ok".to_string(),
                Ok(Err(err)) => format!("err: {err:?}"),
                Err(err) => format!("cache err: {err:?}"),
            };
            if dur < min_dur { min_dur = dur; }
        }
        println!("{vm_kind:?} {name}: took {min_dur:#.2?} ({last_status})");
    }

    Ok(())
}
