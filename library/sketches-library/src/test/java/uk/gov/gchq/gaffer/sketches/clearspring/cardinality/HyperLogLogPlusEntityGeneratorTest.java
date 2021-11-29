package uk.gov.gchq.gaffer.sketches.clearspring.cardinality;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.sketches.CardinalityEntityGenerator;
import uk.gov.gchq.gaffer.sketches.datasketches.cardinality.HllSketchEntityGenerator;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class HyperLogLogPlusEntityGeneratorTest extends FunctionTest {

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
        final HyperLogLogPlusEntityGenerator hyperLogLogPlusEntityGenerator = new HyperLogLogPlusEntityGenerator();
        // When
        final String json = new String(JSONSerialiser.serialise(hyperLogLogPlusEntityGenerator));
        HyperLogLogPlusEntityGenerator deserialisedHyperLogLogPlusEntityGenerator = JSONSerialiser.deserialise(json, HyperLogLogPlusEntityGenerator.class);
        // Then
        assertEquals(hyperLogLogPlusEntityGenerator, deserialisedHyperLogLogPlusEntityGenerator);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.sketches.clearspring.cardinality.HyperLogLogPlusEntityGenerator\"}", json);
    }


    @Override
    protected HyperLogLogPlusEntityGenerator getInstance() {
        return new HyperLogLogPlusEntityGenerator();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}