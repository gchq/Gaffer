package uk.gov.gchq.gaffer.sketches.datasketches.cardinality.function;

import com.yahoo.sketches.hll.HllSketch;
import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ToHllSketchTest extends FunctionTest<ToHllSketch> {

    @Test
    void shouldCreateNonBoundedSet() {
        // // Given
        // final ToHllSketch toHllSketch =
        //         new ToHllSketch();
        // // When
        // HllSketch result = toHllSketch.apply("Input");

        // // Then
        // TimestampSet expected = new RBMBackedTimestampSet.Builder()
        //         .timeBucket(CommonTimeUtil.TimeBucket.DAY)
        //         .build();
        // expected.add(Instant.ofEpochMilli(TEST_TIMESTAMPS));

        // assertEquals(expected, result);
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Object.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{HllSketch.class};
    }


    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final ToHllSketch toHllSketch =
                new ToHllSketch();
        // When
        final String json = new String(JSONSerialiser.serialise(toHllSketch));
        ToHllSketch deserialisedToHllSketch = JSONSerialiser.deserialise(json, ToHllSketch.class);
        // Then
        assertEquals(toHllSketch, deserialisedToHllSketch);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.sketches.datasketches.cardinality.function.ToHllSketch\"}", json );
    }

    @Override
    protected ToHllSketch getInstance() {
        return new ToHllSketch();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}
