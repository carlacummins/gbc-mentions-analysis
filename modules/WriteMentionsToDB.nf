process WRITE_TO_DB {
    tag "write_to_db.chunk_${meta.chunk}"
    label 'process_tiny'
    debug true

    input:
    tuple val(meta), path(classifications), path(resource_metadata)

    // output:
    // tuple val(meta), path("resource_mentions_summary.csv"), emit: classifications

    script:
    """
    write_mentions_to_db.py --classifications ${classifications} --metadata ${resource_metadata} ${task.ext.args}
    """
}