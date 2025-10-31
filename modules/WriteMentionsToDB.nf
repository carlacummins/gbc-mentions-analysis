process WRITE_TO_DB {
    tag "write_to_db.chunk_${meta.chunk}"
    label 'process_single'
    // debug true

    input:
    tuple val(meta), path(classifications)
    path(texts_metadata_dir)
    path(resources_json)

    // output:
    // tuple val(meta), path("resource_mentions_summary.csv"), emit: classifications

    script:
    """
    write_mentions_to_db.py --classifications ${classifications} --metadata-dir ${texts_metadata_dir} --resources ${resources_json} ${task.ext.args}
    """
}