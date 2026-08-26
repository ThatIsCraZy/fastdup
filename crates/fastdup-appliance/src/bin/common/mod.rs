use fastdup_store::MetadataGarbageCollectionReport;

pub(crate) fn metadata_gc_status_fields(
    report: MetadataGarbageCollectionReport,
    prefix: &str,
) -> String {
    let metrics = report.metrics();
    format!(
        concat!(
            "{prefix}objects_removed={} {prefix}bytes_removed={} ",
            "{prefix}objects_retained={} {prefix}mark_mode={} ",
            "{prefix}exact_reason={} {prefix}exact_mark_performed={} ",
            "{prefix}catalog_generation={} {prefix}wall_us={} ",
            "{prefix}barrier_wait_us={} {prefix}object_graph_read_bytes={} ",
            "{prefix}candidate_read_bytes={} {prefix}catalog_read_bytes={} ",
            "{prefix}catalog_write_bytes={} {prefix}unlinked_bytes={} ",
            "{prefix}root_syncs={} {prefix}catalog_chain_runs={}"
        ),
        report.objects_removed(),
        report.bytes_removed(),
        report.objects_retained(),
        report.mark_mode().as_str(),
        report
            .exact_reason()
            .map_or("none", fastdup_store::MetadataGcExactReason::as_str),
        report.exact_mark_performed(),
        report
            .catalog_generation()
            .map_or_else(|| "none".to_owned(), |generation| generation.to_string()),
        metrics.wall().as_micros(),
        metrics.barrier_wait().as_micros(),
        metrics.object_graph_read_bytes(),
        metrics.candidate_read_bytes(),
        metrics.catalog_read_bytes(),
        metrics.catalog_write_bytes(),
        metrics.unlinked_bytes(),
        metrics.root_syncs(),
        metrics.catalog_chain_runs(),
        prefix = prefix,
    )
}
