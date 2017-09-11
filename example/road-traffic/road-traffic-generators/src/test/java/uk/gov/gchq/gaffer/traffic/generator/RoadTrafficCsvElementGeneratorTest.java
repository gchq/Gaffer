package uk.gov.gchq.gaffer.traffic.generator;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToManyElementGenerator;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RoadTrafficCsvElementGeneratorTest {

    @Test
    public void shouldParseSampleData() {

        // Given
        final OneToManyElementGenerator<CSVRecord> generator = new RoadTrafficCsvElementGenerator();

        try {
            final FileReader reader = new FileReader(getClass().getResource("/roadTrafficSampleData.csv").getFile());
            final Iterable<CSVRecord> csvRecords = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

            // When
            final Iterable<? extends Element> elements = generator.apply(csvRecords);

            int entityCount = 0;
            int edgeCount = 0;
            for (final Element element : elements) {
                if (element instanceof Entity) {
                    entityCount++;
                } else if (element instanceof Edge) {
                    edgeCount++;
                } else {
                    fail("Unrecognised element class: " + element.getClassName());
                }
            }

            assertEquals(166240, entityCount);
            assertEquals(72730, edgeCount);

        } catch (final IOException e) {
            e.printStackTrace();
        }

    }

}
