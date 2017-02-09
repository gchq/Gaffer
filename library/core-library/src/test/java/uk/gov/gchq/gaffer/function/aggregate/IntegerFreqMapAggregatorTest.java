package uk.gov.gchq.gaffer.function.aggregate;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.AggregateFunctionTest;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.IntegerFreqMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class IntegerFreqMapAggregatorTest extends AggregateFunctionTest {
    @Test
    public void shouldMergeFreqMaps() {
        // Given
        final IntegerFreqMapAggregator aggregator = new IntegerFreqMapAggregator();
        aggregator.init();

        final IntegerFreqMap freqMap1 = new IntegerFreqMap();
        freqMap1.put("1", 2);
        freqMap1.put("2", 3);

        final IntegerFreqMap freqMap2 = new IntegerFreqMap();
        freqMap2.put("2", 4);
        freqMap2.put("3", 5);

        // When
        aggregator._aggregate(freqMap1);
        aggregator._aggregate(freqMap2);

        // Then
        final IntegerFreqMap mergedFreqMap = ((IntegerFreqMap) aggregator.state()[0]);
        assertEquals((Integer) 2, mergedFreqMap.get("1"));
        assertEquals((Integer) 7, mergedFreqMap.get("2"));
        assertEquals((Integer) 5, mergedFreqMap.get("3"));
    }

    @Test
    public void shouldCloneAggregator() {
        // Given
        final IntegerFreqMapAggregator aggregator = new IntegerFreqMapAggregator();
        final IntegerFreqMap freqMap1 = new IntegerFreqMap();
        freqMap1.put("1", 2);
        freqMap1.put("2", 3);
        aggregator._aggregate(freqMap1);

        // When
        final IntegerFreqMapAggregator clone = aggregator.statelessClone();

        // Then
        assertNotSame(aggregator, clone);
        assertNull((clone.state()[0]));
    }


    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final IntegerFreqMapAggregator aggregator = new IntegerFreqMapAggregator();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));

        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.aggregate.IntegerFreqMapAggregator\"%n" +
                "}"), json);

        // When 2
        final IntegerFreqMapAggregator deserialisedAggregator = new JSONSerialiser().deserialise(json.getBytes(), getFunctionClass());

        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected IntegerFreqMapAggregator getInstance() {
        return new IntegerFreqMapAggregator();
    }

    @Override
    protected Class<IntegerFreqMapAggregator> getFunctionClass() {
        return IntegerFreqMapAggregator.class;
    }
}
