package uk.gov.gchq.gaffer.flink.operation.handler;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;

public class CsvToElement implements OneToOneElementGenerator<String> {
    @Override
    public Element _apply(final String csv) {
        if (null == csv) {
            System.err.println("CSV is required in format [source],[destination]");
            return null;
        }
        final String[] parts = csv.split(",");
        if (2 != parts.length) {
            System.err.println("CSV is required in format [source],[destination]");
            return null;
        }

        return new Edge.Builder()
                .group("edge")
                .source(parts[0].trim())
                .dest(parts[1].trim())
                .directed(true)
                .property("count", 1)
                .build();
    }
}
