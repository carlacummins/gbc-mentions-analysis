include { QUERY_EUROPEPMC              } from '../modules/QueryEuropePMC.nf'
include { FETCH_AND_PREPROCESS_ARTICLE } from '../modules/FetchAndPreprocessArticle.nf'

workflow PREPARE_TEXTS {
    take:
        resources_json

    main:
        query = QUERY_EUROPEPMC(resources_json)

		query.idlists
        | flatten
		| map { idlist ->
			def this_meta = [:]
            def matcher = (idlist =~ /pmc_idlist\.chunk_(\d+)\.txt/)
            if (matcher.find()) {
                this_meta.chunk = matcher.group(1)  // store as string, or `.toInteger()` if you like
            }
            tuple(this_meta, idlist)
        }
		| set {idlist_chunks}

        article_text_dirs = FETCH_AND_PREPROCESS_ARTICLE(idlist_chunks)

    emit:
        text_dirs = article_text_dirs
        metadata_dir = query.metadata_dir
}

workflow {
    PREPARE_TEXTS(params.resource_list)
}
