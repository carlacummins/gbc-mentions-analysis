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
        resources_json = FETCH_RESOURCE_LIST()

        // This version of the subworkflow fetches EuropePMC full text articles and preprocesses them.
        // Replace with your own data source as required.
        texts = PREPARE_TEXTS(resources_json)

        // fan out chunks and classify each batch of texts
        classified_texts = CLASSIFY_TEXTS(texts.text_dirs, resources_json)

        // Write each classification to DB (separately per chunk)
        WRITE_TO_DB(classified_texts.classifications, texts.metadata, resources_json)

        // Collect all resource counts and merge/collate
        classified_texts.resource_counts
        | map { _meta, file -> file }   // drop meta
        | collect()
        | RESOURCE_SPECIFICITY_SCORES
}