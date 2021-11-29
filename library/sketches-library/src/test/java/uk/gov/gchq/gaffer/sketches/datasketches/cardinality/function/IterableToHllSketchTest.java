package uk.gov.gchq.gaffer.sketches.datasketches.cardinality.function;

import com.yahoo.sketches.hll.HllSketch;
import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.sketches.datasketches.cardinality.HllSketchEntityGenerator;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class IterableToHllSketchTest extends FunctionTest {

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Iterable.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{HllSketch.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final IterableToHllSketch iterableToHllSketch = new IterableToHllSketch();
        // When
        final String json = new String(JSONSerialiser.serialise(iterableToHllSketch));
        IterableToHllSketch deserialisedIterableToHllSketch = JSONSerialiser.deserialise(json, IterableToHllSketch.class);
        // Then
        assertEquals(iterableToHllSketch, deserialisedIterableToHllSketch);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.sketches.datasketches.cardinality.function.IterableToHllSketch\"}", json);
    }



    @Override
    protected IterableToHllSketch getInstance() {
        return new IterableToHllSketch();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}