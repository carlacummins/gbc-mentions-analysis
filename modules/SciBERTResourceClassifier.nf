process SCIBERT_RESOURCE_CLASSIFIER {
    tag "scibert_resource_classifier.chunk_${meta.chunk}"
    label 'process_gpu'
    debug true

    input:
    tuple val(meta), path(input_dir)
    path(resources)

    output:
    tuple val(meta), path(mentions_out), emit: classifications
    tuple val(meta), path(counts_out), emit: resource_counts

    script:
    mentions_out = "resource_mentions_summary.${meta.chunk}.csv"
    counts_out = "prediction_counts.${meta.chunk}.pkl"
    """
    classify_resource_mentions.py --indir ${input_dir} --resources ${resources} --mentions_out ${mentions_out} --counts_out ${counts_out} ${task.ext.args}
    """
}