package uk.gov.gchq.gaffer.operation.impl.function;

import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.impl.Reduce;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public class ReduceTest extends OperationTest<Reduce> {
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final Iterable<Integer> input = Arrays.asList(1, 2, 3);

        final Reduce<Integer> reduce = new Reduce.Builder<Integer>()
                .input(input)
                .identity(0)
                .aggregateFunction(new Sum())
                .build();

        // Then
        assertNotNull(reduce.getInput());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Iterable<Integer> input = Arrays.asList(1, 2, 3);

        final Reduce<Integer> reduce = new Reduce.Builder<Integer>()
                .input(input)
                .identity(0)
                .aggregateFunction(new Sum())
                .build();

        // When
        final Reduce<Integer> clone = reduce.shallowClone();

        // Then
        assertNotSame(reduce, clone);
        assertEquals(new Integer(1), clone.getInput().iterator().next());
    }

    @Override
    protected Reduce getTestObject() {
        final Reduce reduce = new Reduce(new Sum());
        reduce.setIdentity(0);
        return reduce;
    }
}
