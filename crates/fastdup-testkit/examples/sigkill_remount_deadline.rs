use std::path::PathBuf;

use fastdup_testkit::SigkillRemountConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1).map(PathBuf::from);
    let daemon = arguments
        .next()
        .ok_or("usage: sigkill_remount_deadline DAEMON NEW_RUN_ROOT")?;
    let run_root = arguments
        .next()
        .ok_or("usage: sigkill_remount_deadline DAEMON NEW_RUN_ROOT")?;
    if arguments.next().is_some() {
        return Err("usage: sigkill_remount_deadline DAEMON NEW_RUN_ROOT".into());
    }

    let report = SigkillRemountConfig::v1(daemon, run_root).run()?;
    for (ordinal, case) in report.cases().iter().copied().enumerate() {
        println!(
            "case={} kill_delay_ms={} acknowledged_to_kill_ms={} acknowledged_records={} required_records={} recovered_records={} recovered_file_present={} deadline_required={}",
            ordinal,
            case.kill_delay().as_millis(),
            case.acknowledged_to_kill().as_millis(),
            case.acknowledged_records(),
            case.required_records(),
            case.recovered_records(),
            case.recovered_file_present(),
            case.deadline_required(),
        );
    }
    println!(
        "sigkill_remount_ok=true cases={} durability_window_ms={} evidence_root={}",
        report.cases().len(),
        report.durability_window().as_millis(),
        report.run_root().display(),
    );
    Ok(())
}
