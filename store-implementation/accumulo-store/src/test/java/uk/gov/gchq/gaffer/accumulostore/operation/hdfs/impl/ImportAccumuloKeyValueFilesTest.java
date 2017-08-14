package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.impl;


import com.google.common.collect.Sets;
import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation.ImportAccumuloKeyValueFiles;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class ImportAccumuloKeyValueFilesTest extends OperationTest<ImportAccumuloKeyValueFiles> {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    private static final String INPUT_DIRECTORY = "/input";
    private static final String FAIL_DIRECTORY = "/fail";
    private static final String TEST_OPTION_KEY = "testOption";

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("failurePath", "inputPath");
    }

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
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

    @Override
    protected ImportAccumuloKeyValueFiles getTestObject() {
        return new ImportAccumuloKeyValueFiles();
    }
}
