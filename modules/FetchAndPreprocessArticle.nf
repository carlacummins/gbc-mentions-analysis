process FETCH_AND_PREPROCESS_ARTICLE {
    tag "fetch_and_preprocess_article.chunk_${meta.chunk}"
    label 'process_tiny'
    debug true

    input:
    tuple val(meta), val(idlist), path(local_xmls_path)

    output:
    tuple val(meta), path(outdir), emit: results_dir

    script:
    outdir = "article_texts.${meta.chunk}"
    """
    fetch_and_preprocess_article.py --idlist ${idlist} --outdir ${outdir} ${task.ext.args}
    """
}