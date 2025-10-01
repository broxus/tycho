use std::io;
use std::mem::size_of;

#[cfg(target_os = "linux")]
use procfs::process::Process;
use thiserror::Error;

/// Target CPUs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cpu(pub Vec<u32>);

impl Cpu {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_slice(&self) -> &[u32] {
        &self.0
    }
}

#[derive(Debug, Error)]
pub enum AffinityError {
    #[error("empty CPU list")]
    EmptyCpuList,
    #[error("cpu {cpu} out of range (valid: 0..={max})")]
    CpuOutOfRange { cpu: usize, max: usize },
    #[error(transparent)]
    Os(#[from] io::Error),
    #[error("CPU affinity not supported on this platform")]
    Unsupported,
}

#[cfg(target_os = "linux")]
/// Wrapper around <https://man7.org/linux/man-pages/man2/sched_setaffinity.2.html>
pub fn set_affinity(thread_id: libc::pid_t, cpu: Cpu) -> Result<(), AffinityError> {
    // Capacity that a cpu_set_t can represent.
    let cap = libc::CPU_SETSIZE as usize;

    // Initialize an empty set.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe { libc::CPU_ZERO(&mut set) };

    let mut add = |c: u32| -> Result<(), AffinityError> {
        let idx = c as usize;
        if idx >= cap {
            return Err(AffinityError::CpuOutOfRange {
                cpu: idx,
                max: cap - 1,
            });
        }
        unsafe { libc::CPU_SET(idx, &mut set) };
        Ok(())
    };

    if cpu.0.is_empty() {
        return Err(AffinityError::EmptyCpuList);
    }
    for c in cpu.0 {
        add(c)?;
    }

    // sched_setaffinity(pid==0 targets the calling thread).
    let r = unsafe {
        libc::sched_setaffinity(
            thread_id,
            size_of::<libc::cpu_set_t>(),
            &set as *const libc::cpu_set_t,
        )
    };
    if r != 0 {
        return Err(io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
/// Wrapper around <https://man7.org/linux/man-pages/man2/sched_setaffinity.2.html>
pub fn get_affinity(thread_id: libc::pid_t) -> Result<Cpu, AffinityError> {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    let r = unsafe {
        libc::sched_getaffinity(
            thread_id,
            size_of::<libc::cpu_set_t>(),
            &mut set as *mut libc::cpu_set_t,
        )
    };
    if r != 0 {
        return Err(io::Error::last_os_error().into());
    }
    let mut cpus = Vec::new();
    let cap = libc::CPU_SETSIZE as usize;
    for idx in 0..cap {
        if unsafe { libc::CPU_ISSET(idx, &set) } {
            cpus.push(idx as u32);
        }
    }
    if cpus.is_empty() {
        return Err(AffinityError::EmptyCpuList);
    }
    Ok(Cpu(cpus))
}

#[derive(Debug, Clone)]
pub struct ThreadInfo {
    pub tgid: i32,
    pub tid: i32,
    pub ppid: i32,
    pub name: String,
    /// Flattened list of CPUs from `cpus_allowed_list`
    pub cpus_allowed_list: Vec<u32>,
    /// Single-letter state ('R','S','D','Z',…)
    pub state: char,
}

#[derive(Debug, Error)]
pub enum ThreadScanError {
    #[error(transparent)]
    Proc(#[from] procfs::ProcError),
    #[error("thread scan not supported on this platform")]
    Unsupported,
}

pub fn match_threads<F>(pid: usize, mut matcher: F) -> Result<Vec<i32>, ThreadScanError>
where
    F: FnMut(&ThreadInfo) -> bool,
{
    let proc = Process::new(pid as i32)?;
    let mut out = Vec::new();

    for t in proc.tasks()? {
        let t = t?;
        // /proc/<pid>/task/<tid>/status
        let st = t.status()?; // has name, state, tgid, pid (tid), ppid, cpus_allowed_list, …
        let name = st.name;
        let state = st.state.chars().next().unwrap_or('?');
        let cpus_allowed_list = st
            .cpus_allowed_list
            .unwrap_or_default()
            .into_iter()
            .flat_map(|(lo, hi)| lo..=hi)
            .collect::<Vec<_>>();

        let info = ThreadInfo {
            tgid: st.tgid,
            tid: st.pid, // this is the TID in Status
            ppid: st.ppid,
            name,
            cpus_allowed_list,
            state,
        };

        if matcher(&info) {
            out.push(t.tid);
        }
    }

    out.sort_unstable();
    out.dedup();
    Ok(out)
}

#[cfg(not(target_os = "linux"))]
pub fn set_affinity(_thread_id: Tid, _cpu: Cpu) -> Result<(), AffinityError> {
    Err(AffinityError::Unsupported)
}

#[cfg(not(target_os = "linux"))]
pub fn get_affinity(_thread_id: Tid) -> Result<Cpu, AffinityError> {
    Err(AffinityError::Unsupported)
}

#[cfg(not(target_os = "linux"))]
pub fn match_threads<F>(_pid: i32, _matcher: F) -> Result<Vec<Tid>, ThreadScanError>
where
    F: FnMut(&ThreadInfo) -> bool,
{
    Err(ThreadScanError::Unsupported)
}

#[cfg(test)]
#[cfg(target_os = "linux")]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_set_affinity() {
        let current_thread_id = unsafe { libc::gettid() };
        let original = get_affinity(current_thread_id).unwrap();
        let desired = Cpu(vec![0]);
        set_affinity(current_thread_id, desired.clone()).unwrap();
        let fetched = get_affinity(current_thread_id).unwrap();
        assert_eq!(fetched, desired);
        set_affinity(current_thread_id, original).unwrap();
    }

    #[test]
    fn set_affinity_accepts_single_mask() {
        let current_thread_id = unsafe { libc::gettid() };
        let snapshot = get_affinity(current_thread_id).unwrap();
        set_affinity(current_thread_id, Cpu(vec![0])).unwrap();
        let fetched = get_affinity(current_thread_id).unwrap();
        assert_eq!(fetched.as_slice(), &[0]);
        set_affinity(current_thread_id, snapshot).unwrap();
    }

    #[test]
    fn set_affinity_rejects_empty_list() {
        let current_thread_id = unsafe { libc::gettid() };
        let result = set_affinity(current_thread_id, Cpu(Vec::new()));
        assert!(matches!(result, Err(AffinityError::EmptyCpuList)));
    }

    #[test]
    fn set_affinity_rejects_out_of_range() {
        let current_thread_id = unsafe { libc::gettid() };
        let capacity = libc::CPU_SETSIZE as usize;
        let result = set_affinity(current_thread_id, Cpu(vec![capacity as u32]));
        match result.unwrap_err() {
            AffinityError::CpuOutOfRange { cpu, max } => {
                assert_eq!(cpu, capacity);
                assert_eq!(max, capacity - 1);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn get_affinity_reflects_current_mask() {
        let current_thread_id = unsafe { libc::gettid() };
        let original = get_affinity(current_thread_id).unwrap();
        let desired = Cpu(vec![0]);
        set_affinity(current_thread_id, desired.clone()).unwrap();
        let fetched = get_affinity(current_thread_id).unwrap();
        assert_eq!(fetched, desired);
        set_affinity(current_thread_id, original).unwrap();
    }

    #[test]
    fn set_for_status() -> anyhow::Result<()> {
        let thread_id = Arc::new(AtomicI32::new(0));

        {
            let thread_id = thread_id.clone();
            std::thread::Builder::new()
                .name("kekw".to_string())
                .spawn(move || {
                    thread_id.store(unsafe { libc::gettid() }, Ordering::Relaxed);
                    std::thread::sleep(Duration::from_secs(1231232));
                })?;
        };

        std::thread::sleep(Duration::from_millis(10));
        let current_thread_id = unsafe { libc::gettid() };
        let found = match_threads(current_thread_id as _, |info| info.name == "kekw")?;
        assert_eq!(found.len(), 1);
        let found = *found.first().unwrap();
        assert_eq!(found, thread_id.load(Ordering::Relaxed));

        let num_cpus = std::thread::available_parallelism()?.get();
        let original = get_affinity(found as _)?;
        assert_eq!(original.len(), num_cpus);

        let expected = Cpu(vec![0]);
        set_affinity(found as _, expected.clone())?;
        let got_affinity = get_affinity(found as _)?;

        assert_eq!(got_affinity, expected);
        set_affinity(found as _, original)?;

        Ok(())
    }
}
