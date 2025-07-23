process FETCH_AND_PREPROCESS_ARTICLE {
    label 'process_tiny'
    debug true

    input:
    val idlist
    path local_xmls_path

    output:
    path(outdir), emit: results_dir

    script:
    outdir = idlist.replaceAll('.txt', '')
    """
    fetch_and_preprocess_article.py --local_xml_dir ${local_xmls_path} --idlist ${idlist} --outdir ${outdir}
    """
}