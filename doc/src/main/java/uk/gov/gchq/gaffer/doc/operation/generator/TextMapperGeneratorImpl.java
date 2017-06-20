package uk.gov.gchq.gaffer.doc.operation.generator;

import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.TextMapperGenerator;

public class TextMapperGeneratorImpl extends TextMapperGenerator {
    public TextMapperGeneratorImpl() {
        super(new ElementGenerator());
    }
}
