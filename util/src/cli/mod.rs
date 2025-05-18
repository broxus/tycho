use std::ffi::CStr;
use std::net::IpAddr;
pub mod config;

pub mod logger;

pub mod signal;

pub mod metrics;

pub async fn resolve_public_ip(ip: Option<IpAddr>) -> anyhow::Result<IpAddr> {
    match ip {
        Some(address) => Ok(address),
        None => match getip::addr_v4().await {
            Ok(address) => Ok(IpAddr::V4(address)),
            Err(e) => anyhow::bail!("failed to resolve public IP address: {e:?}"),
        },
    }
}

pub fn tune_jemalloc_decay_times(new_decay_ms: isize) -> Result<(), tikv_jemalloc_ctl::Error> {
    tracing::info!(
        target: "memory_tuning",
        new_decay_ms,
        "Attempting to set jemalloc decay times."
    );

    unsafe fn read_and_log(key: &'static CStr, field: &'static str) {
        match tikv_jemalloc_ctl::raw::read::<isize>(key.to_bytes_with_nul()) {
            Ok(val) => {
                tracing::info!(
                    target: "memory_tuning",
                    field,
                    value = val,
                    "Current value of {field}."
                );
            }
            Err(e) => {
                tracing::warn!(
                    target: "memory_tuning",
                    field,
                    error = ?e,
                    "Could not read current {field}."
                );
            }
        }
    }

    // For options that are returned as a const char* (like opt.thp)
    unsafe fn read_and_log_str(key: &'static CStr, field: &'static str) {
        match tikv_jemalloc_ctl::raw::read_str(key.to_bytes_with_nul()) {
            Ok(str) => {
                tracing::info!(
                    target: "memory_tuning",
                    field,
                    value = str,
                    "Runtime-configured value of {field}."
                );
            }
            Err(e) => {
                tracing::warn!(
                    target: "memory_tuning",
                    field,
                    error = ?e,
                    "Could not read value of {field}."
                );
            }
        }
    }

    unsafe fn write_and_log(
        key: &'static CStr,
        field: &'static str,
        val: isize,
    ) -> Result<(), tikv_jemalloc_ctl::Error> {
        tikv_jemalloc_ctl::raw::write(key.to_bytes_with_nul(), val).inspect_err(|e| {
            tracing::error!(
                target: "memory_tuning",
                field,
                error = ?e,
                value = val,
                "Failed to set {field}."
            );
        })?;
        tracing::info!(
            target: "memory_tuning",
            field,
            new_value = val,
            "Successfully set {field}."
        );
        Ok(())
    }

    unsafe {
        read_and_log(c"arenas.dirty_decay_ms", "dirty_decay_ms");
        read_and_log(c"arenas.muzzy_decay_ms", "muzzy_decay_ms");
        read_and_log_str(c"opt.thp", "opt.thp"); // Read from ctl, not from static
        write_and_log(c"arenas.dirty_decay_ms", "dirty_decay_ms", new_decay_ms)?;
        write_and_log(c"arenas.muzzy_decay_ms", "muzzy_decay_ms", new_decay_ms)?;
    }

    tracing::info!(
        target: "memory_tuning",
        new_dirty_decay_ms = new_decay_ms,
        new_muzzy_decay_ms = new_decay_ms,
        "Jemalloc decay times successfully updated."
    );
    Ok(())
}
