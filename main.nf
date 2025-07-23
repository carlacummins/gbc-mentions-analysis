nextflow.enable.dsl=2

include { FETCH_RESOURCE_LIST         } from './modules/FetchResourceList.nf'
include { WRITE_TO_DB                 } from './modules/WriteMentionsToDB.nf'
include { RESOURCE_SPECIFICITY_SCORES } from './modules/ResourceSpecificityScores.nf'

include { PREPARE_TEXTS  } from './subworkflows/PrepareTexts.nf'
include { CLASSIFY_TEXTS } from './subworkflows/ClassifyTexts.nf'
// include { WRITE_RESULTS } from './subworkflows/WriteResults.nf'

// Run the workflow
workflow {
    main:
        (meta_ch, resources_json_ch) = FETCH_RESOURCE_LIST([:]) // meta data, e.g., { name: 'example' }

        // This version of the subworkflow fetches EuropePMC full text articles and preprocesses them.
        // Replace with your own data source as required.
        texts = PREPARE_TEXTS(meta_ch, resources_json_ch)

        // fan out chunks and classify each batch of texts
        texts.text_dirs
        | map { meta, text_dir ->
            tuple(meta, text_dir, params.model, resources_json_ch)
        } | CLASSIFY_TEXTS
        | set { classified_texts }

        // Write each classification to DB (separately per chunk)
        classified_texts.classifications
        | map { meta, classification ->
            tuple(meta, classification, texts.metadata)
        }
        | WRITE_TO_DB

        // Collect all resource counts and merge/collate
        classified_texts.resource_counts
        | map { _meta, file -> file }   // drop meta
        | collect()
        | RESOURCE_SPECIFICITY_SCORES
}