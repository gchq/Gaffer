package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.impl;


import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation.SplitTable;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;

import static org.junit.Assert.assertEquals;

public class SplitTableTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    private static final String INPUT_DIRECTORY = "/input";
    private static final String TEST_OPTION_KEY = "testOption";

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final SplitTable op = new SplitTable();
        op.setInputPath(INPUT_DIRECTORY);

        // When
        byte[] json = serialiser.serialise(op, true);

        final SplitTable deserialisedOp = serialiser.deserialise(json, SplitTable.class);

        // Then
        assertEquals(INPUT_DIRECTORY, deserialisedOp.getInputPath());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final SplitTable splitTable = new SplitTable.Builder().inputPath(INPUT_DIRECTORY).option(TEST_OPTION_KEY, "true").build();
        assertEquals(INPUT_DIRECTORY, splitTable.getInputPath());
        assertEquals("true", splitTable.getOption(TEST_OPTION_KEY));
    }
}
