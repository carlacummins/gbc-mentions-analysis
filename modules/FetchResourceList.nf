// Fetches the resource list from the database and expands with provided aliases (wrapper for fetch_resource_list.py)

process FETCH_RESOURCE_LIST {
    tag "fetch_resource_list"
    label 'process_tiny'
    // debug true

    input:
    path(aliases_json)

    output:
    path("resource_list.json"), emit: resource_list

    script:
    """
    fetch_resource_list.py --out resource_list.json --aliases ${aliases_json} ${task.ext.args}
    """
}