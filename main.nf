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
        resource_list = FETCH_RESOURCE_LIST(
            params.resource_sql,
            params.aliases_json
        )

        // This version of the subworkflow fetches EuropePMC full text articles and preprocesses them.
        // Replace with your own data source as required.
        texts = PREPARE_TEXTS(
            params.chunks,
            resource_list,
            params.local_xmls_path
        )
        // texts_metadata = texts.metadata

        texts.text_dirs
        | map { text_dir ->
            [text_dir, params.model, resource_list]
        } | CLASSIFY_TEXTS
        | set { classified_texts }

        // WRITE_RESULTS(classified_texts.classifications, classified_texts.resource_counts)

        // Write each classification to DB separately
        classified_texts.classifications
        | map { classification ->
            [classification, texts.metadata, params.dry_run, params.db_credentials_json]
        }
        | WRITE_TO_DB

        // Collect all resource counts and merge/collate
        classified_texts.resource_counts
        | collect()
        | RESOURCE_SPECIFICITY_SCORES
}


// workflow {
//   fetch_epmc()

//   classify_mentions(fetch_epmc.chunks)

//   load_to_db(classify_mentions.true_mentions, fetch_epmc.metadata)

//   aggregate_counts(classify_mentions.mention_counts)
// }