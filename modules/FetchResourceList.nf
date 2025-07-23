process FETCH_RESOURCE_LIST {
    label 'process_tiny'
    debug true

    input:
    val resource_sql
    path aliases_json

    output:
    path("resource_list.json"), emit: resource_list

    script:
    """
    fetch_resource_list.py --sql ${resource_sql} --aliases ${aliases_json} --out resource_list.json
    """
}