package uk.gov.gchq.gaffer.traffic.generator;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToManyElementGenerator;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RoadTrafficStringElementGeneratorTest {

    @Test
    public void shouldParseSampleData() {

        // Given
        final OneToManyElementGenerator<String> generator = new RoadTrafficStringElementGenerator();

        try {
            final File file = new File(getClass().getResource("/roadTrafficSampleData.csv").getFile());

            final LineIterator iterator = FileUtils.lineIterator(file);

            // When
            final Iterable<? extends Element> elements = generator.apply(() -> iterator);

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

            assertEquals(20780, entityCount);
            assertEquals(72730, edgeCount);

        } catch (final IOException e) {
            e.printStackTrace();
        }

    }

}
