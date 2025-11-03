// Queries Europe PMC for articles containing provided resource names (wrapper for query_europepmc.py)

process QUERY_EUROPEPMC {
    tag "query_europepmc"
    label 'process_medium'
    // debug true

    input:
    path(resources_json)

    output:
    path("epmc_results/metadata/"), emit: metadata_dir
    path("epmc_results/pmc_idlist.chunk_*.txt"), emit: idlists

    script:
    """
    query_europepmc.py --outdir epmc_results --workers ${task.cpus} --resources ${resources_json} ${task.ext.args}
    """
}
