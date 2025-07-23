include { QUERY_EUROPEPMC              } from '../modules/QueryEuropePMC.nf'
include { FETCH_AND_PREPROCESS_ARTICLE } from '../modules/FetchAndPreprocessArticle.nf'

workflow PREPARE_TEXTS {
    take:
        meta
        resources_json

    main:
        query = QUERY_EUROPEPMC(tuple(meta, resources_json))
        // query.idlists | flatten
        // | view

        query.idlists
        | map { idlist ->
            def this_meta = meta.clone()
            def matcher = (idlist.name =~ /pmc_idlist\.chunk_(\d+)\.txt/)
            if (matcher.find()) {
                this_meta.chunk = matcher.group(1)  // store as string, or `.toInteger()` if you like
            }
            tuple(this_meta, idlist)
        }
        | FETCH_AND_PREPROCESS_ARTICLE
        | set { article_text_dirs }

    emit:
        text_dirs = article_text_dirs
        metadata = query.metadata
}

workflow {
    PREPARE_TEXTS(params.meta, params.resource_list)
}