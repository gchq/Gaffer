package uk.gov.gchq.gaffer.types.function;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.FreqMap;

import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;


class CountsToFreqMapTest extends FunctionTest {

    @Override
    protected CountsToFreqMap getInstance() {
        return new CountsToFreqMap();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return CountsToFreqMap.class;
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{ List.class };
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[] { FreqMap.class };
    }

    @Override
    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final CountsToFreqMap countsToFreqMap = new CountsToFreqMap();
        // When
        final String json = new String(JSONSerialiser.serialise(countsToFreqMap));
        CountsToFreqMap deserialisedCountsToFreqMap = JSONSerialiser.deserialise(json, CountsToFreqMap.class);
        // Then
        assertEquals(countsToFreqMap, deserialisedCountsToFreqMap);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.types.function.CountsToFreqMap\"}", json);
    }

    @Test
    public void shouldCreateIncompleteFreqMapIfValuesAreMissing() {
        // Given
        CountsToFreqMap function = new CountsToFreqMap(Arrays.asList("a", "b", "c"));

        // When
        FreqMap freqMap = function.apply(Arrays.asList(1L, 2L));

        // Then
        assertEquals(2, freqMap.size());
        assertEquals(1, freqMap.get("a"));
        assertEquals(2, freqMap.get("b"));
    }
}