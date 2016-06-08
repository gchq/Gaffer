package gaffer.accumulostore.operation.hdfs.impl;


import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.OperationTest;
import org.junit.Test;


import static org.junit.Assert.assertEquals;

public class ImportAccumuloKeyValueFilesTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    private static final String INPUT_DIRECTORY = "/input";
    private static final String FAIL_DIRECTORY = "/fail";
    private static final String TEST_OPTION_KEY = "testOption";


    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final ImportAccumuloKeyValueFiles op = new ImportAccumuloKeyValueFiles();
        op.setInputPath(INPUT_DIRECTORY);
        op.setFailurePath(FAIL_DIRECTORY);

        // When
        byte[] json = serialiser.serialise(op, true);

        final ImportAccumuloKeyValueFiles deserialisedOp = serialiser.deserialise(json, ImportAccumuloKeyValueFiles.class);

        // Then
        assertEquals(INPUT_DIRECTORY, deserialisedOp.getInputPath());
        assertEquals(FAIL_DIRECTORY, deserialisedOp.getFailurePath());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final ImportAccumuloKeyValueFiles importAccumuloKeyValueFiles = new ImportAccumuloKeyValueFiles.Builder().inputPath(INPUT_DIRECTORY).failurePath(FAIL_DIRECTORY).option(TEST_OPTION_KEY, "true").build();
        importAccumuloKeyValueFiles.getInputPath();
        importAccumuloKeyValueFiles.getFailurePath();
        assertEquals("true", importAccumuloKeyValueFiles.getOption(TEST_OPTION_KEY));
    }
}
