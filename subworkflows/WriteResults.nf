include { WRITE_MENTIONS_TO_DB } from '../modules/WriteMentionsToDB.nf'

workflow WRITE_RESULTS {
    take:
        classifications
        resource_counts

    main:
        write = WRITE_MENTIONS_TO_DB(classifications, resource_counts)

    emit:
        write
}

workflow {
    WRITE_RESULTS(params.classifications, params.resource_counts)
}