include { WRITE_TO_DB } from '../modules/WriteMentionsToDB.nf'

workflow WRITE_RESULTS {
    take:
        classifications
        texts_metadata
        resource_counts

    main:
        write = WRITE_TO_DB(classifications, texts_metadata, resource_counts)

    emit:
        write
}

workflow {
    WRITE_RESULTS(params.classifications, params.texts_metadata, params.resource_counts)
}