process FETCH_RESOURCE_LIST {
    tag "fetch_resource_list"
    label 'process_tiny'
    debug true

    input:
    val(meta)

    output:
    tuple val(meta), path("resource_list.json"), emit: resource_list

    script:
    """
    fetch_resource_list.py --out resource_list.json ${task.ext.args}
    """
}