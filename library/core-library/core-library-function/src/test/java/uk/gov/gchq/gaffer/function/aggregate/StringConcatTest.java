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

public class StringConcatTest extends AggregateFunctionTest {
    @Test
    public void shouldConcatStringsTogether() {
        // Given
        final StringConcat aggregator = new StringConcat();
        aggregator.setSeparator(";");
        aggregator.init();

        // When
        aggregator._aggregate("1");
        aggregator.aggregate(new Object[]{"2"});
        aggregator.aggregate(new Object[]{null});

        // Then
        assertEquals("1;2", aggregator.state()[0]);
    }

    @Test
    public void shouldCloneAggregator() {
        // Given
        final StringConcat aggregator = new StringConcat();
        aggregator._aggregate("1");

        // When
        final StringConcat clone = aggregator.statelessClone();

        // Then
        assertNotSame(aggregator, clone);
        assertNull(clone.state()[0]);
    }


    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final StringConcat aggregator = new StringConcat();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));

        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.aggregate.StringConcat\",%n" +
                "  \"separator\" : \",\"%n" +
                "}"), json);

        // When 2
        final StringConcat deserialisedAggregator = new JSONSerialiser().deserialise(json.getBytes(), getFunctionClass());

        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected StringConcat getInstance() {
        return new StringConcat();
    }

    @Override
    protected Class<StringConcat> getFunctionClass() {
        return StringConcat.class;
    }
}
