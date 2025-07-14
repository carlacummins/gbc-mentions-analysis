nextflow.enable.dsl=2

// Run the workflow
include { QUERY_EUROPEPMC } from './modules/queryEuropePMC'

workflow {
    main:
        query = QUERY_EUROPEPMC(params.chunks)

        query.results | flatten
        | view
}


// workflow {
//   fetch_epmc()

//   classify_mentions(fetch_epmc.chunks)

//   load_to_db(classify_mentions.true_mentions, fetch_epmc.metadata)

//   aggregate_counts(classify_mentions.mention_counts)
// }