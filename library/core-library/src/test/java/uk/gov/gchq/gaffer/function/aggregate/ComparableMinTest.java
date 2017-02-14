package uk.gov.gchq.gaffer.function.aggregate;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.AggregateFunctionTest;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class ComparableMinTest extends AggregateFunctionTest {
    @Test
    public void shouldReturnMinimumValue() {
        // Given
        final ComparableMin aggregator = getInstance();
        aggregator.init();

        // When
        aggregator._aggregate(3);
        aggregator._aggregate(1);
        aggregator._aggregate(2);

        // Then
        assertEquals(1, aggregator.state()[0]);
    }

    @Test
    public void shouldCloneAggregator() {
        // Given
        final ComparableMin aggregator = getInstance();
        aggregator._aggregate(1);

        // When
        final ComparableMin clone = aggregator.statelessClone();

        // Then
        assertNotSame(aggregator, clone);
        assertNull(clone.state()[0]);
    }


    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final ComparableMin aggregator = getInstance();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));

        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.aggregate.ComparableMin\"%n" +
                "}"), json);

        // When 2
        final ComparableMin deserialisedAggregator = new JSONSerialiser().deserialise(json.getBytes(), getFunctionClass());

        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected ComparableMin getInstance() {
        return new ComparableMin();
    }

    @Override
    protected Class<ComparableMin> getFunctionClass() {
        return ComparableMin.class;
    }
}
