process QUERY_EUROPEPMC {
    label 'process_tiny'
    debug true

    input:
    val chunks
    path resources_json

    output:
    path("epmc_results/article_metadata.json"), emit: metadata
    path("epmc_results/**.txt"), emit: idlists

    script:
    """
    query_europepmc.py --outdir epmc_results --chunks ${chunks} --resources ${resources_json}
    """
}