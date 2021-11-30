package uk.gov.gchq.gaffer.sketches.datasketches.cardinality.function;

import com.yahoo.sketches.hll.HllSketch;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.sketches.datasketches.cardinality.HllSketchEntityGenerator;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class IterableToHllSketchTest extends FunctionTest {

    @Test
    public void shouldCreateEmptyWhenNull() {
        //Given
        IterableToHllSketch iterableToHllSketch = new IterableToHllSketch();

        //When
        HllSketch result = iterableToHllSketch.apply(null);

        //Then
        assertThat(result.getEstimate()).isEqualTo(0);
    }

    @Test
    public void shouldCreateHllSketch() {
        //Given
        IterableToHllSketch iterableToHllSketch = new IterableToHllSketch();
        List<Object> input = Arrays.asList("one", "two", "three", "four", "five");

        //When
        HllSketch result = iterableToHllSketch.apply(input);

        //Then
        assertThat(result.getEstimate()).isCloseTo(5, Percentage.withPercentage(0.001));
    }

    @Test
    public void shouldCreateHllSketchCardinality() {
        //Given
        IterableToHllSketch iterableToHllSketch = new IterableToHllSketch();
        List<Object> input = Arrays.asList("one", "one", "two", "two", "three");

        //When
        HllSketch result = iterableToHllSketch.apply(input);

        //Then
        assertThat(result.getEstimate()).isCloseTo(3, Percentage.withPercentage(0.001));
    }


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