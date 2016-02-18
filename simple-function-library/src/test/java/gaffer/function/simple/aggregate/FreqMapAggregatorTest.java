package gaffer.function.simple.aggregate;

import gaffer.exception.SerialisationException;
import gaffer.function.ConsumerProducerFunctionTest;
import gaffer.function.simple.types.FreqMap;
import gaffer.jsonserialisation.JSONSerialiser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class FreqMapAggregatorTest extends ConsumerProducerFunctionTest {
    @Test
    public void shouldMergeFreqMaps() {
        // Given
        final FreqMapAggregator aggregator = new FreqMapAggregator();
        aggregator.init();

        final FreqMap freqMap1 = new FreqMap();
        freqMap1.put("1", 2);
        freqMap1.put("2", 3);

        final FreqMap freqMap2 = new FreqMap();
        freqMap2.put("2", 4);
        freqMap2.put("3", 5);

        // When
        aggregator._aggregate(freqMap1);
        aggregator._aggregate(freqMap2);

        // Then
        final FreqMap mergedFreqMap = ((FreqMap) aggregator.state()[0]);
        assertEquals((Integer) 2, mergedFreqMap.get("1"));
        assertEquals((Integer) 7, mergedFreqMap.get("2"));
        assertEquals((Integer) 5, mergedFreqMap.get("3"));
    }

    @Test
    public void shouldCloneAggregator() {
        // Given
        final FreqMapAggregator aggregator = new FreqMapAggregator();
        final FreqMap freqMap1 = new FreqMap();
        freqMap1.put("1", 2);
        freqMap1.put("2", 3);
        aggregator._aggregate(freqMap1);

        // When
        final FreqMapAggregator clone = aggregator.statelessClone();

        // Then
        assertNotSame(aggregator, clone);
        assertTrue(((FreqMap) clone.state()[0]).isEmpty());
    }


    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final FreqMapAggregator aggregator = new FreqMapAggregator();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));

        // Then 1
        assertEquals("{\n" +
                "  \"class\" : \"gaffer.function.simple.aggregate.FreqMapAggregator\"\n" +
                "}", json);

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
