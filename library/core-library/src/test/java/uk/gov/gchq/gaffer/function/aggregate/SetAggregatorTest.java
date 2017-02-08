package uk.gov.gchq.gaffer.function.aggregate;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.AggregateFunctionTest;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.util.HashSet;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class SetAggregatorTest extends AggregateFunctionTest {
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

        SetAggregator<String> aggregator = new SetAggregator<>();
        aggregator.init();

        // When
        aggregator._aggregate(treeSet1);
        aggregator._aggregate(treeSet2);

        // Then
        assertEquals(TreeSet.class, aggregator._state().getClass());
        assertEquals(expectedResult, aggregator._state());
    }

    @Test
    public void shouldAggregateHashSetsTogether() {
        // Given
        final HashSet<Integer> hashSet1 = new HashSet<>();
        hashSet1.add(1);

        final HashSet<Integer> hashSet2 = new HashSet<>();
        hashSet2.add(2);
        hashSet2.add(3);

        final HashSet<Integer> expectedResult = new HashSet<>();
        expectedResult.add(1);
        expectedResult.add(2);
        expectedResult.add(3);

        SetAggregator<Integer> aggregator = new SetAggregator<>();
        aggregator.init();

        // When
        aggregator._aggregate(hashSet1);
        aggregator._aggregate(hashSet2);

        // Then
        assertEquals(HashSet.class, aggregator._state().getClass());
        assertEquals(expectedResult, aggregator._state());
    }

    @Test
    public void shouldReturnNullStateIfAggregateNulls() {
        // Given
        SetAggregator<String> aggregator = new SetAggregator<>();
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

        SetAggregator<String> aggregator = new SetAggregator<>();
        aggregator._aggregate(treeSet1);

        // When
        SetAggregator<String> clone = aggregator.statelessClone();

        // Then
        assertNotSame(aggregator, clone);
        assertNull(clone._state());
    }


    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        SetAggregator<String> aggregator = new SetAggregator<>();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));

        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.aggregate.SetAggregator\"%n" +
                "}"), json);

        // When 2
        SetAggregator<String> deserialisedAggregator = new JSONSerialiser().deserialise(json.getBytes(), getFunctionClass());

        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected SetAggregator getInstance() {
        return new SetAggregator<>();
    }

    @Override
    protected Class<SetAggregator> getFunctionClass() {
        return SetAggregator.class;
    }
}
