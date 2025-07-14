process QUERY_EUROPEPMC {
    label 'process_tiny'
    debug true

    input:
    val chunks

    output:
    path("epmc_results/**.json"), emit: results

    script:
    """
    query_europepmc.py --outdir epmc_results --chunks ${chunks}
    """
}