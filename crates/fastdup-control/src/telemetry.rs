use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::{DiskTelemetry, RepositoryState, SeriesPoint, TelemetrySnapshot};

#[derive(Clone, Copy, Debug, Default)]
struct CpuCounters {
    busy: u64,
    total: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct DiskCounters {
    reads: u64,
    read_sectors: u64,
    writes: u64,
    write_sectors: u64,
    outstanding: u64,
    io_millis: u64,
}

#[derive(Debug)]
pub struct SystemSampler {
    proc_root: PathBuf,
    sys_root: PathBuf,
    previous_cpu: CpuCounters,
    previous_disks: BTreeMap<String, DiskCounters>,
    previous_at: Instant,
    sequence: u64,
    series: VecDeque<SeriesPoint>,
    frontend_read_mbps: f64,
    frontend_write_mbps: f64,
    previous_frontend_bytes: Option<(u64, u64)>,
    previous_frontend_at: Instant,
    exact_hit_bytes: u64,
    new_chunk_bytes: u64,
    reduction_ratio: f64,
    repository_state: RepositoryState,
    metadata_kernel_name: Option<String>,
    data_kernel_name: Option<String>,
    data_path: Option<PathBuf>,
}

impl Default for SystemSampler {
    fn default() -> Self {
        Self::new("/proc", "/sys/class/block")
    }
}

impl SystemSampler {
    #[must_use]
    pub fn new(proc_root: impl Into<PathBuf>, sys_root: impl Into<PathBuf>) -> Self {
        Self {
            proc_root: proc_root.into(),
            sys_root: sys_root.into(),
            previous_cpu: CpuCounters::default(),
            previous_disks: BTreeMap::new(),
            previous_at: Instant::now(),
            sequence: 0,
            series: VecDeque::with_capacity(900),
            frontend_read_mbps: 0.0,
            frontend_write_mbps: 0.0,
            previous_frontend_bytes: None,
            previous_frontend_at: Instant::now(),
            exact_hit_bytes: 0,
            new_chunk_bytes: 0,
            reduction_ratio: 0.0,
            repository_state: RepositoryState::Uninitialized,
            metadata_kernel_name: None,
            data_kernel_name: None,
            data_path: None,
        }
    }

    pub fn configure_repository(
        &mut self,
        state: RepositoryState,
        metadata_kernel_name: Option<String>,
        data_kernel_name: Option<String>,
        data_path: Option<PathBuf>,
    ) {
        self.repository_state = state;
        self.metadata_kernel_name = metadata_kernel_name;
        self.data_kernel_name = data_kernel_name;
        self.data_path = data_path;
    }

    pub fn update_frontend(
        &mut self,
        read_mbps: f64,
        write_mbps: f64,
        exact_hit_bytes: u64,
        new_chunk_bytes: u64,
        reduction_ratio: f64,
    ) {
        self.frontend_read_mbps = read_mbps.max(0.0);
        self.frontend_write_mbps = write_mbps.max(0.0);
        self.exact_hit_bytes = exact_hit_bytes;
        self.new_chunk_bytes = new_chunk_bytes;
        self.reduction_ratio = reduction_ratio.max(0.0);
    }

    pub fn update_frontend_counters(&mut self, read_bytes: u64, write_bytes: u64) {
        let now = Instant::now();
        if let Some((previous_read, previous_write)) = self.previous_frontend_bytes {
            let elapsed = now
                .duration_since(self.previous_frontend_at)
                .as_secs_f64()
                .max(0.001);
            self.frontend_read_mbps =
                read_bytes.saturating_sub(previous_read) as f64 / 1_000_000.0 / elapsed;
            self.frontend_write_mbps =
                write_bytes.saturating_sub(previous_write) as f64 / 1_000_000.0 / elapsed;
        }
        self.previous_frontend_bytes = Some((read_bytes, write_bytes));
        self.previous_frontend_at = now;
    }

    pub fn update_reduction(
        &mut self,
        exact_hit_bytes: u64,
        new_chunk_bytes: u64,
        logical_chunk_bytes: u64,
        physical_container_bytes: u64,
    ) {
        self.exact_hit_bytes = exact_hit_bytes;
        self.new_chunk_bytes = new_chunk_bytes;
        self.reduction_ratio = if physical_container_bytes == 0 {
            0.0
        } else {
            logical_chunk_bytes as f64 / physical_container_bytes as f64
        };
    }

    pub fn sample(&mut self) -> TelemetrySnapshot {
        let now = Instant::now();
        let elapsed = now
            .duration_since(self.previous_at)
            .as_secs_f64()
            .max(0.001);
        self.previous_at = now;
        self.sequence = self.sequence.saturating_add(1);
        let observed_at = timestamp();
        let cpu = read_cpu(&self.proc_root.join("stat"));
        let cpu_percent = cpu_percent(self.previous_cpu, cpu);
        self.previous_cpu = cpu;
        let ram_percent = read_ram_percent(&self.proc_root.join("meminfo"));
        let mut disks = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.sys_root) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with("loop") || name.starts_with("ram") || name.starts_with("sr") {
                    continue;
                }
                let current = read_disk_counters(&entry.path().join("stat"));
                let previous = self
                    .previous_disks
                    .insert(name.clone(), current)
                    .unwrap_or(current);
                disks.push(disk_telemetry(
                    &entry.path(),
                    &name,
                    role_for(
                        &name,
                        self.metadata_kernel_name.as_deref(),
                        self.data_kernel_name.as_deref(),
                    ),
                    previous,
                    current,
                    elapsed,
                ));
            }
        }
        let point = SeriesPoint {
            time: observed_at.clone(),
            read: self.frontend_read_mbps,
            write: self.frontend_write_mbps,
        };
        if self.series.len() == 900 {
            self.series.pop_front();
        }
        self.series.push_back(point);
        let (data_used_bytes, data_capacity_bytes) = self
            .data_path
            .as_deref()
            .and_then(filesystem_usage)
            .unwrap_or((0, 0));
        TelemetrySnapshot {
            sequence: self.sequence,
            observed_at,
            repository_state: self.repository_state.clone(),
            commit_generation: None,
            frontend_read_mbps: self.frontend_read_mbps,
            frontend_write_mbps: self.frontend_write_mbps,
            dedup_rate: dedup_rate(self.exact_hit_bytes, self.new_chunk_bytes),
            reduction_ratio: self.reduction_ratio,
            cpu_percent,
            ram_percent,
            data_used_bytes,
            data_capacity_bytes,
            last_checkpoint_seconds: None,
            disks,
            series: self.series.iter().cloned().collect(),
        }
    }
}

#[must_use]
pub fn dedup_rate(exact_hit_bytes: u64, new_chunk_bytes: u64) -> f64 {
    let total = exact_hit_bytes.saturating_add(new_chunk_bytes);
    if total == 0 {
        0.0
    } else {
        exact_hit_bytes as f64 * 100.0 / total as f64
    }
}

fn read_cpu(path: &Path) -> CpuCounters {
    let Ok(contents) = std::fs::read_to_string(path) else {
        return CpuCounters::default();
    };
    let Some(line) = contents.lines().find(|line| line.starts_with("cpu ")) else {
        return CpuCounters::default();
    };
    let values = line
        .split_ascii_whitespace()
        .skip(1)
        .filter_map(|value| value.parse::<u64>().ok())
        .collect::<Vec<_>>();
    let total = values.iter().copied().fold(0_u64, u64::saturating_add);
    let idle = values
        .get(3)
        .copied()
        .unwrap_or(0)
        .saturating_add(values.get(4).copied().unwrap_or(0));
    CpuCounters {
        busy: total.saturating_sub(idle),
        total,
    }
}

fn cpu_percent(previous: CpuCounters, current: CpuCounters) -> f64 {
    let total = current.total.saturating_sub(previous.total);
    if total == 0 {
        0.0
    } else {
        current.busy.saturating_sub(previous.busy) as f64 * 100.0 / total as f64
    }
}

fn read_ram_percent(path: &Path) -> f64 {
    let Ok(contents) = std::fs::read_to_string(path) else {
        return 0.0;
    };
    let values = contents
        .lines()
        .filter_map(|line| {
            let (name, rest) = line.split_once(':')?;
            let value = rest.split_ascii_whitespace().next()?.parse::<u64>().ok()?;
            Some((name, value))
        })
        .collect::<BTreeMap<_, _>>();
    let total = values.get("MemTotal").copied().unwrap_or(0);
    let available = values.get("MemAvailable").copied().unwrap_or(0);
    if total == 0 {
        0.0
    } else {
        total.saturating_sub(available) as f64 * 100.0 / total as f64
    }
}

fn read_disk_counters(path: &Path) -> DiskCounters {
    let Ok(contents) = std::fs::read_to_string(path) else {
        return DiskCounters::default();
    };
    let values = contents
        .split_ascii_whitespace()
        .filter_map(|value| value.parse::<u64>().ok())
        .collect::<Vec<_>>();
    DiskCounters {
        reads: values.first().copied().unwrap_or(0),
        read_sectors: values.get(2).copied().unwrap_or(0),
        writes: values.get(4).copied().unwrap_or(0),
        write_sectors: values.get(6).copied().unwrap_or(0),
        outstanding: values.get(8).copied().unwrap_or(0),
        io_millis: values.get(9).copied().unwrap_or(0),
    }
}

fn disk_telemetry(
    path: &Path,
    name: &str,
    role: String,
    previous: DiskCounters,
    current: DiskCounters,
    elapsed: f64,
) -> DiskTelemetry {
    let read_sectors = current.read_sectors.saturating_sub(previous.read_sectors);
    let write_sectors = current.write_sectors.saturating_sub(previous.write_sectors);
    let read_mbps = read_sectors as f64 * 512.0 / 1_000_000.0 / elapsed;
    let write_mbps = write_sectors as f64 * 512.0 / 1_000_000.0 / elapsed;
    let utilization =
        current.io_millis.saturating_sub(previous.io_millis) as f64 / (elapsed * 10.0);
    let capacity_sectors = read_u64(&path.join("size"));
    let rotational = read_u64(&path.join("queue/rotational")) == 1;
    DiskTelemetry {
        id: name.to_owned(),
        role,
        model: read_trimmed(&path.join("device/model")).unwrap_or_else(|| name.to_owned()),
        kind: if name.starts_with("nvme") {
            "NVMe SSD"
        } else if rotational {
            "HDD"
        } else {
            "SSD"
        }
        .to_owned(),
        capacity_bytes: capacity_sectors.saturating_mul(512),
        hba_port: "nicht verfügbar".to_owned(),
        outstanding_io: current.outstanding,
        read_mbps,
        write_mbps,
        read_iops: current.reads.saturating_sub(previous.reads) as f64 / elapsed,
        write_iops: current.writes.saturating_sub(previous.writes) as f64 / elapsed,
        utilization: utilization.clamp(0.0, 100.0),
    }
}

fn role_for(name: &str, metadata: Option<&str>, data: Option<&str>) -> String {
    if Some(name) == metadata {
        "Metadata".to_owned()
    } else if Some(name) == data {
        "DATA".to_owned()
    } else {
        "System".to_owned()
    }
}

fn read_u64(path: &Path) -> u64 {
    read_trimmed(path)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn read_trimmed(path: &Path) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_owned())
}

fn filesystem_usage(path: &Path) -> Option<(u64, u64)> {
    let statistics = rustix::fs::statvfs(path).ok()?;
    let fragment = statistics.f_frsize.max(1);
    let capacity = statistics.f_blocks.checked_mul(fragment)?;
    let available = statistics.f_bavail.checked_mul(fragment)?;
    Some((capacity.saturating_sub(available), capacity))
}

fn timestamp() -> String {
    let now = time::OffsetDateTime::now_utc();
    now.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| crate::unix_seconds().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_rate_excludes_fill_and_recipe_reuse_by_construction() {
        assert!((dedup_rate(750, 250) - 75.0).abs() < f64::EPSILON);
        assert!(dedup_rate(0, 0).abs() < f64::EPSILON);
    }
}
