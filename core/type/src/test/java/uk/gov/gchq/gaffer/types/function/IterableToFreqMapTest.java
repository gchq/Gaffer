package uk.gov.gchq.gaffer.types.function;

import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class IterableToFreqMapTest extends FunctionTest {

    @Test
    void shouldInitialiseTheValueOfTheKeyToOneIfNotSeenBefore() {
        // Given
        Iterable<String> strings = (Iterable<String>) Arrays.asList("one");
        final IterableToFreqMap iterableToFreqMap =
                new IterableToFreqMap();

        // When
        FreqMap result =  iterableToFreqMap.apply(strings);

        // Then
        FreqMap expected = new FreqMap("one");
        assertEquals(expected, result);
    }

    @Test
    void shouldIncrementTheValueOfTheKeyByOne() {
        // Given
        Iterable<String> strings = (Iterable<String>) Arrays.asList("one", "one");
        final IterableToFreqMap iterableToFreqMap =
                new IterableToFreqMap();

        // When
        FreqMap result =  iterableToFreqMap.apply(strings);

        // Then
        HashMap<String, Long> input = new HashMap<>();
        input.put("one", 2L);
        FreqMap expected = new FreqMap(input);
        assertEquals(expected, result);
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Iterable.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{FreqMap.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final IterableToFreqMap iterableToFreqMap = new IterableToFreqMap();
        // When
        final String json = new String(JSONSerialiser.serialise(iterableToFreqMap));
        IterableToFreqMap deserialisedIterableToFreqMap = JSONSerialiser.deserialise(json, IterableToFreqMap.class);
        // Then
        assertEquals(iterableToFreqMap, deserialisedIterableToFreqMap);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.types.function.IterableToFreqMap\"}", json);
    }

    @Override
    protected IterableToFreqMap getInstance() {
        return new IterableToFreqMap();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}