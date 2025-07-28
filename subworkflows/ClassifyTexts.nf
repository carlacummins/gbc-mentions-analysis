include { SCIBERT_RESOURCE_CLASSIFIER } from '../modules/SciBERTResourceClassifier.nf'

workflow CLASSIFY_TEXTS {
    take:
        meta_text_ch // this is a tuple of (meta, text_dir)
        resources

    main:
        classifier = SCIBERT_RESOURCE_CLASSIFIER(meta_text_ch, resources)

    emit:
        classifications = classifier.classifications
        resource_counts = classifier.resource_counts
}

workflow {
    CLASSIFY_TEXTS(params.meta_text_ch, params.resource_list)
}