process WRITE_TO_DB {
    label 'process_tiny'
    debug true

    input:
    path classifications
    path resource_counts

    output:
    path("resource_mentions_summary.csv"), emit: classifications
    path("prediction_counts.pkl"), emit: resource_counts

    script:
    """
    write_mentions_to_db.py --classifications ${classifications} --counts ${resource_counts}
    """
}