package gaffer.accumulostore.operation.hdfs.impl;


import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.OperationTest;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ImportAccumuloKeyValueFilesTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        String inputDir = "/input";
        String failurePath = "/fail";
        final ImportAccumuloKeyValueFiles op = new ImportAccumuloKeyValueFiles();
        op.setInputPath(inputDir);
        op.setFailurePath(failurePath);

        // When
        byte[] json = serialiser.serialise(op, true);

        final ImportAccumuloKeyValueFiles deserialisedOp = serialiser.deserialise(json, ImportAccumuloKeyValueFiles.class);

        // Then
        assertEquals(inputDir, deserialisedOp.getInputPath());
        assertEquals(failurePath, deserialisedOp.getFailurePath());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        String inputPath = "/input";
        String failurePath = "/fail";
        ImportAccumuloKeyValueFiles importAccumuloKeyValueFiles = new ImportAccumuloKeyValueFiles.Builder().inputPath(inputPath).failurePath(failurePath).option("testOption", "true").build();
        importAccumuloKeyValueFiles.getInputPath();
        importAccumuloKeyValueFiles.getFailurePath();
        assertEquals("true", importAccumuloKeyValueFiles.getOption("testOption"));
    }
}
