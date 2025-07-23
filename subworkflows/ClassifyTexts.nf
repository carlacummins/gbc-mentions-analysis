include { SCIBERT_RESOURCE_CLASSIFIER } from '../modules/SciBERTResourceClassifier.nf'

workflow CLASSIFY_TEXTS {
    take:
        text
        model
        resources

    main:
        classifier = SCIBERT_RESOURCE_CLASSIFIER(text, model, resources)

    emit:
        classifications = classifier.classifications
        resource_counts = classifier.resource_counts
}

workflow {
    CLASSIFY_TEXTS(params.text_dirs, params.model, params.resource_list)
}