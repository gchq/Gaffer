package uk.gov.gchq.gaffer.function.aggregate;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.AggregateFunctionTest;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.FreqMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class FreqMapAggregatorTest extends AggregateFunctionTest {
    @Test
    public void shouldMergeFreqMaps() {
        // Given
        final FreqMapAggregator aggregator = new FreqMapAggregator();
        aggregator.init();

        final FreqMap freqMap1 = new FreqMap();
        freqMap1.put("1", 2L);
        freqMap1.put("2", 3L);

        final FreqMap freqMap2 = new FreqMap();
        freqMap2.put("2", 4L);
        freqMap2.put("3", 5L);

        // When
        aggregator._aggregate(freqMap1);
        aggregator._aggregate(freqMap2);

        // Then
        final FreqMap mergedFreqMap = ((FreqMap) aggregator.state()[0]);
        assertEquals((Long) 2L, mergedFreqMap.get("1"));
        assertEquals((Long) 7L, mergedFreqMap.get("2"));
        assertEquals((Long) 5L, mergedFreqMap.get("3"));
    }

    @Test
    public void shouldCloneAggregator() {
        // Given
        final FreqMapAggregator aggregator = new FreqMapAggregator();
        final FreqMap freqMap1 = new FreqMap();
        freqMap1.put("1", 2L);
        freqMap1.put("2", 3L);
        aggregator._aggregate(freqMap1);

        // When
        final FreqMapAggregator clone = aggregator.statelessClone();

        // Then
        assertNotSame(aggregator, clone);
        assertNull((clone.state()[0]));
    }


    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final FreqMapAggregator aggregator = new FreqMapAggregator();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));

        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.aggregate.FreqMapAggregator\"%n" +
                "}"), json);

        // When 2
        final FreqMapAggregator deserialisedAggregator = new JSONSerialiser().deserialise(json.getBytes(), getFunctionClass());

        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected FreqMapAggregator getInstance() {
        return new FreqMapAggregator();
    }

    @Override
    protected Class<FreqMapAggregator> getFunctionClass() {
        return FreqMapAggregator.class;
    }
}
