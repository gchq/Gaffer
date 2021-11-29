package uk.gov.gchq.gaffer.sketches.datasketches.cardinality;

import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.TuplesToElements;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class HllSketchEntityGeneratorTest extends FunctionTest {

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Element.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{Iterable.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final HllSketchEntityGenerator hllSketchEntityGenerator = new HllSketchEntityGenerator();
        // When
        final String json = new String(JSONSerialiser.serialise(hllSketchEntityGenerator));
        HllSketchEntityGenerator deserialisedHllSketchEntityGenerator = JSONSerialiser.deserialise(json, HllSketchEntityGenerator.class);
        // Then
        assertEquals(hllSketchEntityGenerator, deserialisedHllSketchEntityGenerator);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.sketches.datasketches.cardinality.HllSketchEntityGenerator\"}", json);
    }

    @Override
    protected HllSketchEntityGenerator getInstance() {
        return new HllSketchEntityGenerator();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}