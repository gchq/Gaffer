package gaffer.function.simple.aggregate;

import gaffer.exception.SerialisationException;
import gaffer.function.ConsumerProducerFunctionTest;
import gaffer.jsonserialisation.JSONSerialiser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class ComparableMaxTest extends ConsumerProducerFunctionTest {
    @Test
    public void shouldConcatArraysTogether() {
        // Given
        final ComparableMax aggregator = new ComparableMax();
        aggregator.init();

        // When
        aggregator._aggregate(1);
        aggregator._aggregate(3);
        aggregator._aggregate(2);

        // Then
        assertEquals(3, aggregator.state()[0]);
    }

    @Test
    public void shouldCloneAggregator() {
        // Given
        final ComparableMax aggregator = new ComparableMax();
        aggregator._aggregate(1);

        // When
        final ComparableMax clone = aggregator.statelessClone();

        // Then
        assertNotSame(aggregator, clone);
        assertNull(clone.state()[0]);
    }


    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final ComparableMax aggregator = new ComparableMax();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));

        // Then 1
        assertEquals("{\n" +
                "  \"class\" : \"gaffer.function.simple.aggregate.ComparableMax\"\n" +
                "}", json);

        // When 2
        final ComparableMax deserialisedAggregator = new JSONSerialiser().deserialise(json.getBytes(), getFunctionClass());

        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected ComparableMax getInstance() {
        return new ComparableMax();
    }

    @Override
    protected Class<ComparableMax> getFunctionClass() {
        return ComparableMax.class;
    }
}
