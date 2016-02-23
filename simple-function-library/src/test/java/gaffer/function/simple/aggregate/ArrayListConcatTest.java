package gaffer.function.simple.aggregate;

import gaffer.exception.SerialisationException;
import gaffer.function.ConsumerProducerFunctionTest;
import gaffer.jsonserialisation.JSONSerialiser;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class ArrayListConcatTest extends ConsumerProducerFunctionTest {
    @Test
    public void shouldConcatArraysTogether() {
        // Given
        final ArrayListConcat aggregator = new ArrayListConcat();
        aggregator.init();

        final ArrayList<Object> list1 = new ArrayList<Object>(Arrays.asList(1, 2, 3));
        final ArrayList<Object> list2 = new ArrayList<Object>(Arrays.asList("3", "4", 5L));

        // When
        aggregator._aggregate(list1);
        aggregator._aggregate(list2);

        // Then
        assertEquals(Arrays.asList(1, 2, 3, "3", "4", 5L), aggregator.state()[0]);
    }

    @Test
    public void shouldCloneAggregator() {
        // Given
        final ArrayListConcat aggregator = new ArrayListConcat();
        aggregator._aggregate(new ArrayList<Object>(Arrays.asList(1)));

        // When
        final ArrayListConcat clone = aggregator.statelessClone();

        // Then
        assertNotSame(aggregator, clone);
        assertTrue(((List) clone.state()[0]).isEmpty());
    }


    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final ArrayListConcat aggregator = new ArrayListConcat();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));

        // Then 1
        assertEquals("{\n" +
                "  \"class\" : \"gaffer.function.simple.aggregate.ArrayListConcat\"\n" +
                "}", json);

        // When 2
        final ArrayListConcat deserialisedAggregator = new JSONSerialiser().deserialise(json.getBytes(), getFunctionClass());

        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected ArrayListConcat getInstance() {
        return new ArrayListConcat();
    }

    @Override
    protected Class<ArrayListConcat> getFunctionClass() {
        return ArrayListConcat.class;
    }
}
