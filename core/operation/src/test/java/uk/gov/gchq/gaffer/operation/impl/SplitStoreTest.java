package uk.gov.gchq.gaffer.operation.impl;

import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class SplitStoreTest extends OperationTest<SplitStore> {
    private static final String INPUT_DIRECTORY = "/input";
    private static final String TEST_OPTION_KEY = "testOption";

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("inputPath");
    }

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final SplitStore op = new SplitStore();
        op.setInputPath(INPUT_DIRECTORY);

        // When
        byte[] json = JSONSerialiser.serialise(op, true);

        final SplitStore deserialisedOp = JSONSerialiser.deserialise(json, SplitStore.class);

        // Then
        assertEquals(INPUT_DIRECTORY, deserialisedOp.getInputPath());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final SplitStore splitTable = new SplitStore.Builder().inputPath(INPUT_DIRECTORY).option(TEST_OPTION_KEY, "true").build();
        assertEquals(INPUT_DIRECTORY, splitTable.getInputPath());
        assertEquals("true", splitTable.getOption(TEST_OPTION_KEY));
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final SplitStore splitStore = new SplitStore.Builder()
                .inputPath(INPUT_DIRECTORY)
                .option(TEST_OPTION_KEY, "false")
                .build();

        // When
        final SplitStore clone = splitStore.shallowClone();

        // Then
        assertNotSame(splitStore, clone);
        assertEquals(INPUT_DIRECTORY, clone.getInputPath());
        assertEquals("false", clone.getOptions().get(TEST_OPTION_KEY));
    }

    @Override
    protected SplitStore getTestObject() {
        return new SplitStore();
    }
}
