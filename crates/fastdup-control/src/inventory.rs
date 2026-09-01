use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Deserialize;
use sha2::{Digest as _, Sha256};

use crate::{BackingDisk, BlockTarget};

#[derive(Debug, thiserror::Error)]
pub enum InventoryError {
    #[error("lsblk failed: {0}")]
    Command(String),
    #[error("lsblk returned malformed JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("hardware inventory I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone, Debug, Default)]
pub struct BlockInventory;

impl BlockInventory {
    pub fn discover(&self) -> Result<Vec<BlockTarget>, InventoryError> {
        let output = Command::new("lsblk")
            .args([
                "--json",
                "--bytes",
                "--output",
                "NAME,KNAME,PATH,TYPE,SIZE,ROTA,RO,MODEL,SERIAL,TRAN,HCTL,MOUNTPOINTS,FSTYPE,WWN",
            ])
            .output()?;
        if !output.status.success() {
            return Err(InventoryError::Command(
                String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            ));
        }
        self.from_lsblk(
            &output.stdout,
            Path::new("/dev/disk/by-path"),
            Path::new("/sys/class/block"),
        )
    }

    pub fn from_lsblk(
        &self,
        json: &[u8],
        by_path: &Path,
        sys_block: &Path,
    ) -> Result<Vec<BlockTarget>, InventoryError> {
        let document: LsblkDocument = serde_json::from_slice(json)?;
        let ports = discover_ports(by_path);
        let mut candidates = Vec::new();
        for device in &document.blockdevices {
            collect_candidates(device, &ports, sys_block, &[], &mut candidates);
        }
        candidates.sort_by(|left, right| left.stable_id.cmp(&right.stable_id));
        let revision = inventory_revision(&candidates);
        for target in &mut candidates {
            target.inventory_revision.clone_from(&revision);
        }
        Ok(candidates)
    }
}

#[derive(Debug, Deserialize)]
struct LsblkDocument {
    blockdevices: Vec<LsblkDevice>,
}

#[derive(Debug, Deserialize)]
struct LsblkDevice {
    name: String,
    kname: String,
    path: String,
    #[serde(rename = "type")]
    device_type: String,
    size: Option<u64>,
    rota: Option<bool>,
    ro: Option<bool>,
    model: Option<String>,
    serial: Option<String>,
    tran: Option<String>,
    hctl: Option<String>,
    mountpoints: Option<Vec<Option<String>>>,
    fstype: Option<String>,
    wwn: Option<String>,
    #[serde(default)]
    children: Vec<LsblkDevice>,
}

fn collect_candidates(
    device: &LsblkDevice,
    ports: &BTreeMap<String, String>,
    sys_block: &Path,
    inherited_backing: &[BackingDisk],
    result: &mut Vec<BlockTarget>,
) {
    let mut backing = inherited_backing.to_vec();
    if device.device_type == "disk" {
        backing = vec![backing_disk(device, ports)];
    }
    if matches!(
        device.device_type.as_str(),
        "disk" | "raid0" | "raid1" | "raid5" | "raid6" | "raid10" | "md" | "lvm"
    ) {
        result.push(target_from_device(device, ports, sys_block, &backing));
    }
    for child in &device.children {
        collect_candidates(child, ports, sys_block, &backing, result);
    }
}

fn target_from_device(
    device: &LsblkDevice,
    ports: &BTreeMap<String, String>,
    sys_block: &Path,
    inherited_backing: &[BackingDisk],
) -> BlockTarget {
    let descendants_in_use = device_in_use(device);
    let holders = sys_block.join(&device.kname).join("holders");
    let has_holders = std::fs::read_dir(holders).is_ok_and(|mut entries| entries.next().is_some());
    let eligible = !device.ro.unwrap_or(true) && !descendants_in_use && !has_holders;
    let reason = if device.ro.unwrap_or(true) {
        Some("Gerät ist schreibgeschützt".to_owned())
    } else if descendants_in_use {
        Some("Gerät oder untergeordnetes Volume ist gemountet beziehungsweise Swap".to_owned())
    } else if has_holders {
        Some("Gerät wird von einem aktiven Block-Layer-Volume verwendet".to_owned())
    } else {
        None
    };
    let serial = clean(device.serial.as_deref());
    let wwn = clean(device.wwn.as_deref());
    let stable_material = if !wwn.is_empty() {
        format!("wwn:{wwn}")
    } else if !serial.is_empty() {
        format!("serial:{serial}")
    } else {
        format!("kernel:{}", device.kname)
    };
    let stable_id = URL_SAFE_NO_PAD.encode(Sha256::digest(stable_material.as_bytes()));
    let hba_port = ports
        .get(&device.kname)
        .cloned()
        .or_else(|| {
            device
                .hctl
                .as_ref()
                .filter(|value| !value.is_empty())
                .map(|value| format!("SCSI {value}"))
        })
        .unwrap_or_else(|| "nicht verfügbar".to_owned());
    let backing_disks = if device.device_type == "disk" {
        vec![backing_disk(device, ports)]
    } else {
        let mut backing = inherited_backing.to_vec();
        backing.extend(collect_backing_disks(device, ports));
        backing.sort_by(|left, right| left.stable_id.cmp(&right.stable_id));
        backing.dedup_by(|left, right| left.stable_id == right.stable_id);
        backing
    };
    BlockTarget {
        stable_id,
        path: device.path.clone(),
        kernel_name: device.kname.clone(),
        model: clean(device.model.as_deref()).if_empty_then(&device.name),
        serial,
        wwn,
        target_type: target_type(device),
        capacity_bytes: device.size.unwrap_or(0),
        hba_port,
        filesystem: device.fstype.clone(),
        eligible,
        eligibility_reason: reason,
        backing_disks,
        inventory_revision: String::new(),
    }
}

fn backing_disk(device: &LsblkDevice, ports: &BTreeMap<String, String>) -> BackingDisk {
    let serial = clean(device.serial.as_deref());
    let wwn = clean(device.wwn.as_deref());
    let material = if !wwn.is_empty() {
        format!("wwn:{wwn}")
    } else if !serial.is_empty() {
        format!("serial:{serial}")
    } else {
        format!("kernel:{}", device.kname)
    };
    BackingDisk {
        stable_id: URL_SAFE_NO_PAD.encode(Sha256::digest(material.as_bytes())),
        kernel_name: device.kname.clone(),
        model: clean(device.model.as_deref()).if_empty_then(&device.name),
        serial,
        hba_port: ports
            .get(&device.kname)
            .cloned()
            .or_else(|| device.hctl.as_ref().map(|value| format!("SCSI {value}")))
            .unwrap_or_else(|| "nicht verfügbar".to_owned()),
    }
}

fn collect_backing_disks(
    device: &LsblkDevice,
    ports: &BTreeMap<String, String>,
) -> Vec<BackingDisk> {
    let mut result = Vec::new();
    for child in &device.children {
        if child.device_type == "disk" {
            result.push(backing_disk(child, ports));
        }
        result.extend(collect_backing_disks(child, ports));
    }
    result
}

fn device_in_use(device: &LsblkDevice) -> bool {
    let self_in_use = device
        .mountpoints
        .as_ref()
        .is_some_and(|mounts| mounts.iter().flatten().any(|mount| !mount.is_empty()));
    self_in_use || device.children.iter().any(device_in_use)
}

fn target_type(device: &LsblkDevice) -> String {
    if device.device_type.starts_with("raid") || device.device_type == "md" {
        return "RAID".to_owned();
    }
    if device.device_type == "lvm" {
        return "LVM".to_owned();
    }
    if device.tran.as_deref() == Some("nvme") || device.kname.starts_with("nvme") {
        return "NVMe SSD".to_owned();
    }
    if device.rota == Some(true) {
        "HDD".to_owned()
    } else {
        "SSD".to_owned()
    }
}

fn discover_ports(directory: &Path) -> BTreeMap<String, String> {
    let mut result = BTreeMap::new();
    let Ok(entries) = std::fs::read_dir(directory) else {
        return result;
    };
    for entry in entries.flatten() {
        let Ok(target) = std::fs::canonicalize(entry.path()) else {
            continue;
        };
        let Some(kernel_name) = target.file_name().and_then(std::ffi::OsStr::to_str) else {
            continue;
        };
        let label = entry.file_name().to_string_lossy().into_owned();
        if !label.contains("-part") {
            result.entry(kernel_name.to_owned()).or_insert(label);
        }
    }
    result
}

fn inventory_revision(targets: &[BlockTarget]) -> String {
    let mut hasher = Sha256::new();
    for target in targets {
        hasher.update(target.stable_id.as_bytes());
        hasher.update(target.capacity_bytes.to_le_bytes());
        hasher.update([u8::from(target.eligible)]);
    }
    URL_SAFE_NO_PAD.encode(hasher.finalize())
}

fn clean(value: Option<&str>) -> String {
    value.unwrap_or_default().trim().to_owned()
}

trait IfEmpty {
    fn if_empty_then(self, fallback: &str) -> String;
}

impl IfEmpty for String {
    fn if_empty_then(self, fallback: &str) -> String {
        if self.is_empty() {
            fallback.to_owned()
        } else {
            self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn excludes_the_system_disk_and_labels_available_targets() {
        let json = br#"{
          "blockdevices": [
            {"name":"sda","kname":"sda","path":"/dev/sda","type":"disk","size":1000,"rota":true,"ro":false,"model":"System","serial":"sys","tran":"sas","hctl":"0:0:0:0","mountpoints":[null],"fstype":null,"wwn":"sys","children":[{"name":"sda1","kname":"sda1","path":"/dev/sda1","type":"part","size":900,"rota":true,"ro":false,"model":null,"serial":null,"tran":null,"hctl":null,"mountpoints":["/"],"fstype":"xfs","wwn":null}]},
            {"name":"sdb","kname":"sdb","path":"/dev/sdb","type":"disk","size":2000,"rota":false,"ro":false,"model":"Metadata","serial":"meta","tran":"nvme","hctl":"0:0:0:2","mountpoints":[null],"fstype":"xfs","wwn":"meta"}
          ]
        }"#;
        let directory = tempfile::tempdir().expect("temporary directory");
        let targets = BlockInventory
            .from_lsblk(
                json,
                &directory.path().join("by-path"),
                &directory.path().join("sys"),
            )
            .expect("parse inventory");
        assert_eq!(targets.len(), 2);
        assert!(
            !targets
                .iter()
                .find(|target| target.path == "/dev/sda")
                .expect("system target")
                .eligible
        );
        let metadata = targets
            .iter()
            .find(|target| target.path == "/dev/sdb")
            .expect("metadata target");
        assert!(metadata.eligible);
        assert_eq!(metadata.target_type, "NVMe SSD");
        assert_eq!(metadata.hba_port, "SCSI 0:0:0:2");
    }

    #[test]
    fn logical_targets_inherit_their_physical_disk_ancestry() {
        let json = br#"{
          "blockdevices": [{
            "name":"sdb","kname":"sdb","path":"/dev/sdb","type":"disk","size":2000,
            "rota":false,"ro":false,"model":"Physical","serial":"disk-1","tran":"sas",
            "hctl":"1:0:0:0","mountpoints":[null],"fstype":null,"wwn":"naa.disk-1",
            "children":[{"name":"vg-data","kname":"dm-2","path":"/dev/mapper/vg-data","type":"lvm",
              "size":1900,"rota":false,"ro":false,"model":null,"serial":null,"tran":null,
              "hctl":null,"mountpoints":[null],"fstype":null,"wwn":null}]
          }]
        }"#;
        let directory = tempfile::tempdir().expect("temporary directory");
        let targets = BlockInventory
            .from_lsblk(
                json,
                &directory.path().join("by-path"),
                &directory.path().join("sys"),
            )
            .expect("parse inventory");
        let disk = targets
            .iter()
            .find(|target| target.kernel_name == "sdb")
            .expect("disk");
        let lvm = targets
            .iter()
            .find(|target| target.kernel_name == "dm-2")
            .expect("lvm");
        assert_eq!(lvm.backing_disks, disk.backing_disks);
        assert_eq!(lvm.backing_disks[0].kernel_name, "sdb");
    }
}
