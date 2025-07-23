include { SCIBERT_RESOURCE_CLASSIFIER } from '../modules/SciBERTResourceClassifier.nf'

workflow CLASSIFY_TEXTS {
    take:
        meta
        text
        model
        resources

    main:
        classifier = SCIBERT_RESOURCE_CLASSIFIER(tuple(meta, text, model, resources))

    emit:
        classifications = classifier.classifications
        resource_counts = classifier.resource_counts
}

workflow {
    CLASSIFY_TEXTS(params.meta, params.text_dirs, params.model, params.resource_list)
}