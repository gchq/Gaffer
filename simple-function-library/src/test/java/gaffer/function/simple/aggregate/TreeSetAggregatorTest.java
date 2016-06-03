package gaffer.function.simple.aggregate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

import gaffer.exception.SerialisationException;
import gaffer.function.AggregateFunctionTest;
import gaffer.jsonserialisation.JSONSerialiser;
import org.junit.Test;
import java.util.TreeSet;

public class TreeSetAggregatorTest extends AggregateFunctionTest {
    @Test
    public void shouldAggregateTreeSetsTogether() {
        // Given
        final TreeSet<String> treeSet1 = new TreeSet<>();
        treeSet1.add("string1");

        final TreeSet<String> treeSet2 = new TreeSet<>();
        treeSet2.add("string3");
        treeSet2.add("string2");

        final TreeSet<String> expectedResult = new TreeSet<>();
        expectedResult.add("string1");
        expectedResult.add("string2");
        expectedResult.add("string3");

        final TreeSetAggregator aggregator = new TreeSetAggregator();
        aggregator.init();

        // When
        aggregator._aggregate(treeSet1);
        aggregator._aggregate(treeSet2);

        // Then
        assertEquals(expectedResult, aggregator._state());
    }

    @Test
    public void shouldReturnNullStateIfAggregateNulls() {
        // Given
        final TreeSetAggregator aggregator = new TreeSetAggregator();
        aggregator.init();

        // When
        aggregator._aggregate(null);
        aggregator._aggregate(null);

        // Then
        assertNull(aggregator._state());
    }

    @Test
    public void shouldCloneAggregator() {
        // Given
        final TreeSet<String> treeSet1 = new TreeSet<>();
        treeSet1.add("string1");

        final TreeSetAggregator aggregator = new TreeSetAggregator();
        aggregator._aggregate(treeSet1);

        // When
        final TreeSetAggregator clone = aggregator.statelessClone();

        // Then
        assertNotSame(aggregator, clone);
        assertNull(clone._state());
    }


    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final TreeSetAggregator aggregator = new TreeSetAggregator();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));

        // Then 1
        assertEquals(String.format("{%n" +
                "  \"class\" : \"gaffer.function.simple.aggregate.TreeSetAggregator\"%n" +
                "}"), json);

        // When 2
        final TreeSetAggregator deserialisedAggregator = new JSONSerialiser().deserialise(json.getBytes(), getFunctionClass());

        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected TreeSetAggregator getInstance() {
        return new TreeSetAggregator();
    }

    @Override
    protected Class<TreeSetAggregator> getFunctionClass() {
        return TreeSetAggregator.class;
    }
}
