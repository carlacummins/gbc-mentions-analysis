process QUERY_EUROPEPMC {
    tag "query_europepmc"
    label 'process_tiny'
    debug true

    input:
    path(resources_json)

    output:
    path("epmc_results/article_metadata.json"), emit: metadata
    path("epmc_results/**.txt"), emit: idlists

    script:
    """
    query_europepmc.py --outdir epmc_results --resources ${resources_json} ${task.ext.args}
    """
}