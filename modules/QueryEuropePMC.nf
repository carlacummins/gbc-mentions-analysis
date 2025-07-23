process QUERY_EUROPEPMC {
    tag "query_europepmc"
    label 'process_tiny'
    debug true

    input:
    tuple val(meta), val(chunks), path(resources_json)

    output:
    tuple val(meta), path("epmc_results/article_metadata.json"), emit: metadata
    tuple val(meta), path("epmc_results/**.txt"), emit: idlists

    script:
    """
    query_europepmc.py --outdir epmc_results --chunks ${chunks} --resources ${resources_json}
    """
}