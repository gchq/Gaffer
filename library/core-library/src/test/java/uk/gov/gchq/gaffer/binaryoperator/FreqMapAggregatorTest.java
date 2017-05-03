package uk.gov.gchq.gaffer.binaryoperator;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class FreqMapAggregatorTest extends BinaryOperatorTest {
    @Test
    public void shouldMergeFreqMaps() {
        // Given
        final FreqMapAggregator aggregator = new FreqMapAggregator();

        final FreqMap freqMap1 = new FreqMap();
        freqMap1.put("1", 2L);
        freqMap1.put("2", 3L);

        final FreqMap freqMap2 = new FreqMap();
        freqMap2.put("2", 4L);
        freqMap2.put("3", 5L);

        // When
        final FreqMap result = aggregator.apply(freqMap1, freqMap2);

        // Then
        assertEquals((Long) 2L, result.get("1"));
        assertEquals((Long) 7L, result.get("2"));
        assertEquals((Long) 5L, result.get("3"));
    }

    @Override
    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final FreqMapAggregator aggregator = new FreqMapAggregator();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));

        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.binaryoperator.FreqMapAggregator\"%n" +
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
