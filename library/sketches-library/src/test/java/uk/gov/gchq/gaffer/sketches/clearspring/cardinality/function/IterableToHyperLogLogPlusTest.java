package uk.gov.gchq.gaffer.sketches.clearspring.cardinality.function;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.HyperLogLogPlusEntityGenerator;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class IterableToHyperLogLogPlusTest extends FunctionTest {

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Iterable.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{HyperLogLogPlus.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final IterableToHyperLogLogPlus iterableToHyperLogLogPlus = new IterableToHyperLogLogPlus();
        // When
        final String json = new String(JSONSerialiser.serialise(iterableToHyperLogLogPlus));
        IterableToHyperLogLogPlus deserialisedIterableToHyperLogLogPlus = JSONSerialiser.deserialise(json, IterableToHyperLogLogPlus.class);
        // Then
        assertEquals(iterableToHyperLogLogPlus, deserialisedIterableToHyperLogLogPlus);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.sketches.clearspring.cardinality.function.IterableToHyperLogLogPlus\"}", json);
    }

    @Override
    protected IterableToHyperLogLogPlus getInstance() {
        return new IterableToHyperLogLogPlus();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}