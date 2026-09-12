use std::{
    env,
    error::Error,
    io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use arc_swap::ArcSwap;
use server::{
    Observability, ServiceRouter,
    algorithms::algorithm_server::create_server,
    config::AppConfig,
    control::ControlServer,
    proxy::{HealthChecker, ProxyServer},
};

fn main() -> Result<(), Box<dyn Error>> {
    let config_path = config_path()?;
    let config = AppConfig::load(&config_path)?;
    let service_registry = Arc::new(config.build_service_registry()?);
    let backend_pool = config.default_backend_pool(&service_registry)?;
    let router = ServiceRouter::new(
        Arc::clone(&service_registry),
        config.server.default_service.clone(),
    )?;
    let observability = Arc::new(Observability::new(
        service_registry
            .all()
            .into_iter()
            .map(|service| service.id.clone()),
        config.observability.log_requests,
    ));
    eprintln!("Loaded server configuration from {}", config_path.display());

    let admin_pool = Arc::clone(&backend_pool);
    let runtime_mode = Arc::new(ArcSwap::from_pointee(config.server.runtime));

    let admin_address = config.server.admin_address.clone();
    let admin_runtime_mode = Arc::clone(&runtime_mode);
    let admin_registry = Arc::clone(&service_registry);
    let admin_observability = Arc::clone(&observability);
    thread::spawn(move || {
        create_server(
            admin_address,
            admin_pool,
            admin_runtime_mode,
            admin_registry,
            admin_observability,
        )
    });

    let control_pool = Arc::clone(&backend_pool);
    let control_address = config.server.control_address.clone();
    let web_root = config.server.web_root.clone();
    let control_registry = Arc::clone(&service_registry);
    let control_observability = Arc::clone(&observability);

    let control_handle = thread::spawn(move || {
        let server = ControlServer::new(
            control_address,
            web_root,
            control_pool,
            control_registry,
            control_observability,
        );

        if let Err(e) = server.run() {
            eprintln!("Control server stopped: {e}");
        }
    });

    let health_shutdown = Arc::new(AtomicBool::new(false));
    let mut health_handles = Vec::new();
    if config.health.enabled {
        let health_config = config.health_check_config();
        for service in service_registry.all() {
            if let Some(pool) = service.proxy_pool() {
                let checker = Arc::new(HealthChecker::new(pool, health_config.clone()));
                health_handles.push(checker.start(Arc::clone(&health_shutdown)));
            }
        }
    }

    let proxy_server = ProxyServer::new(
        config.server.proxy_address.clone(),
        config.server.thread_pool_size,
        Arc::clone(&runtime_mode),
        router,
        observability,
    );

    let proxy_handle = thread::spawn(move || {
        if let Err(e) = proxy_server.run() {
            eprintln!("Proxy server stopped: {e}");
        }
    });

    control_handle.join().unwrap();
    proxy_handle.join().unwrap();
    health_shutdown.store(true, Ordering::Relaxed);
    for handle in health_handles {
        let _ = handle.join();
    }

    eprintln!("Shutting down!");
    Ok(())
}

fn config_path() -> Result<PathBuf, io::Error> {
    let mut arguments = env::args_os().skip(1);
    let first = arguments.next();
    let path = match first {
        None => env::var_os("WEB_SERVER_CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(default_config_path),
        Some(argument) if argument == "--config" || argument == "-c" => arguments
            .next()
            .map(PathBuf::from)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "--config needs a path"))?,
        Some(argument) if !argument.to_string_lossy().starts_with('-') => PathBuf::from(argument),
        Some(argument) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown argument: {}", argument.to_string_lossy()),
            ));
        }
    };

    if let Some(extra) = arguments.next() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unexpected argument: {}", extra.to_string_lossy()),
        ));
    }
    Ok(path)
}

fn default_config_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/server.toml")
}
