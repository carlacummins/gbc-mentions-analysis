include { QUERY_EUROPEPMC              } from '../modules/QueryEuropePMC.nf'
include { FETCH_AND_PREPROCESS_ARTICLE } from '../modules/FetchAndPreprocessArticle.nf'

workflow PREPARE_TEXTS {
    take:
        chunks
        resources_json
        local_xmls_path

    main:
        query = QUERY_EUROPEPMC(chunks, resources_json)
        query.idlists | flatten
        | view

        query.idlists
        | map { idlist ->
            [idlist, local_xmls_path]
        } | FETCH_AND_PREPROCESS_ARTICLE
        | set { article_text_dirs }
        // article_texts = FETCH_AND_PREPROCESS_ARTICLE(query.idlists, local_xmls_path)

    emit:
        text_dirs = article_text_dirs
        metadata = query.metadata
}

workflow {
    PREPARE_TEXTS(params.chunks, params.search, params.local_xmls_path)
}