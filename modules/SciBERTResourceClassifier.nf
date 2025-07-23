process SCIBERT_RESOURCE_CLASSIFIER {
    label 'process_gpu'
    debug true

    input:
    path input_dir
    path model
    path resources

    output:
    path("resource_mentions_summary.csv"), emit: classifications
    path("prediction_counts.pkl"), emit: resource_counts

    script:
    """
    classify_resource_mentions.py --indir ${input_dir} --model ${model} --resources ${resources} --mentions_out resource_mentions_summary.csv --counts_out prediction_counts.pkl
    """
}