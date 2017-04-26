package uk.gov.gchq.gaffer.function.aggregate;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.bifunction.BiFunctionTest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CollectionConcatTest extends BiFunctionTest {
    @Test
    public void shouldConcatArraysTogether() {
        // Given
        final CollectionConcat<Object> aggregator = new CollectionConcat<>();

        final ArrayList<Object> list1 = new ArrayList<>(Arrays.asList(1, 2, 3));
        final ArrayList<Object> list2 = new ArrayList<>(Arrays.asList("3", "4", 5L));

        // When
        final Collection<Object> result = aggregator.apply(list1, list2);

        // Then
        assertEquals(Arrays.asList(1, 2, 3, "3", "4", 5L), result);
    }

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

        CollectionConcat<String> aggregator = new CollectionConcat<>();

        // When
        final Collection<String> result = aggregator.apply(treeSet1, treeSet2);

        // Then
        assertEquals(TreeSet.class, result.getClass());
        assertEquals(expectedResult, result);
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

        CollectionConcat<Integer> aggregator = new CollectionConcat<>();

        // When
        final Collection<Integer> result = aggregator.apply(hashSet1, hashSet2);

        // Then
        assertEquals(HashSet.class, result.getClass());
        assertEquals(expectedResult, result);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final CollectionConcat aggregator = new CollectionConcat();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));

        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.aggregate.CollectionConcat\"%n" +
                "}"), json);

        // When 2
        final CollectionConcat deserialisedAggregator = new JSONSerialiser().deserialise(json.getBytes(), getFunctionClass());

        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected CollectionConcat getInstance() {
        return new CollectionConcat();
    }

    @Override
    protected Class<CollectionConcat> getFunctionClass() {
        return CollectionConcat.class;
    }
}
