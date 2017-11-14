package uk.gov.gchq.gaffer.operation.impl;

import org.junit.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class SplitStoreFromIterableTest extends OperationTest<SplitStoreFromIterable> {
    private static final String TEST_OPTION_KEY = "testOption";

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final SplitStoreFromIterable<String> op = new SplitStoreFromIterable.Builder<String>()
                .input("1", "2", "3")
                .option(TEST_OPTION_KEY, "false")
                .build();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);

        final SplitStoreFromIterable deserialisedOp = JSONSerialiser.deserialise(json, SplitStoreFromIterable.class);

        // Then
        assertEquals(Arrays.asList("1", "2", "3"), deserialisedOp.getInput());
        assertEquals("false", deserialisedOp.getOptions().get(TEST_OPTION_KEY));
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final SplitStoreFromIterable<String> op = new SplitStoreFromIterable.Builder<String>()
                .input("1", "2", "3")
                .option(TEST_OPTION_KEY, "false")
                .build();

        assertEquals(Arrays.asList("1", "2", "3"), op.getInput());
        assertEquals("false", op.getOptions().get(TEST_OPTION_KEY));
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final SplitStoreFromIterable<String> op = new SplitStoreFromIterable.Builder<String>()
                .input("1", "2", "3")
                .option(TEST_OPTION_KEY, "false")
                .build();

        // When
        final SplitStoreFromIterable clone = op.shallowClone();

        // Then
        assertNotSame(op, clone);
        assertEquals(Arrays.asList("1", "2", "3"), op.getInput());
        assertEquals("false", clone.getOptions().get(TEST_OPTION_KEY));
    }

    @Override
    protected SplitStoreFromIterable getTestObject() {
        return new SplitStoreFromIterable();
    }
}
