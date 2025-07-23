process RESOURCE_SPECIFICITY_SCORES {
    tag "resource_specificity_scores"
    label 'process_tiny'
    debug true

    input:
    path(resource_counts)

    output:
    path("resource_specificity_scores.csv"), emit: scores_csv

    script:
    """
    resource_specificity_scores.py ${resource_counts}
    """
}