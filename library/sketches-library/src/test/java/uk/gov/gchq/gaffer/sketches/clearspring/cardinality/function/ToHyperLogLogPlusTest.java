package uk.gov.gchq.gaffer.sketches.clearspring.cardinality.function;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ToHyperLogLogPlusTest extends FunctionTest<ToHyperLogLogPlus> {

    @Test
    public void shouldCreateEmptyWhenNull() {
        //Given
        ToHyperLogLogPlus toHyperLogLogPlus = new ToHyperLogLogPlus();

        //When
        HyperLogLogPlus result = toHyperLogLogPlus.apply(null);

        //Then
        assertThat(result.cardinality()).isEqualTo(0);
    }

    @Test
    public void shouldCreateHyperLogLogPlus() {
        //Given
        ToHyperLogLogPlus toHyperLogLogPlus = new ToHyperLogLogPlus();

        //When
        HyperLogLogPlus result = toHyperLogLogPlus.apply("input");

        //Then
        assertThat(result.cardinality()).isEqualTo(1);
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Object.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{HyperLogLogPlus.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final ToHyperLogLogPlus toHyperLogLogPlus = new ToHyperLogLogPlus();
        // When
        final String json = new String(JSONSerialiser.serialise(toHyperLogLogPlus));
        ToHyperLogLogPlus deserialisedToHyperLogLogPlus = JSONSerialiser.deserialise(json, ToHyperLogLogPlus.class);
        // Then
        assertEquals(toHyperLogLogPlus, deserialisedToHyperLogLogPlus);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.sketches.clearspring.cardinality.function.ToHyperLogLogPlus\"}", json);
    }

    @Override
    protected ToHyperLogLogPlus getInstance() {
        return new ToHyperLogLogPlus();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}