package gaffer.accumulostore.operation.hdfs.impl;


import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.OperationTest;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SampleDataForSplitPointsTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    private static final String INPUT_DIRECTORY = "/input";
    private static final String TEST_OPTION_KEY = "testOption";

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final String resultPath = "/result";
        final SampleDataForSplitPoints op = new SampleDataForSplitPoints();
        op.setInputPaths(Arrays.asList(INPUT_DIRECTORY));
        op.setMapperGeneratorClassName("Test");
        op.setValidate(true);
        op.setProportionToSample(0.1f);
        op.setResultingSplitsFilePath(resultPath);
        op.setNumReduceTasks(10);
        op.setNumMapTasks(5);

        // When
        byte[] json = serialiser.serialise(op, true);

        final SampleDataForSplitPoints deserialisedOp = serialiser.deserialise(json, SampleDataForSplitPoints.class);

        // Then
        assertEquals(INPUT_DIRECTORY, deserialisedOp.getInputPaths().get(0));
        assertEquals(resultPath, deserialisedOp.getResultingSplitsFilePath());
        assertEquals("Test", deserialisedOp.getMapperGeneratorClassName());
        assertTrue(deserialisedOp.isValidate());
        assertEquals(0.1f, deserialisedOp.getProportionToSample(), 1);
        assertEquals(new Integer(5), deserialisedOp.getNumMapTasks());
        assertEquals(new Integer(10), deserialisedOp.getNumReduceTasks());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final SampleDataForSplitPoints sampleDataForSplitPoints = new SampleDataForSplitPoints.Builder().addInputPath(INPUT_DIRECTORY).option(TEST_OPTION_KEY, "true").reducers(10).proportionToSample(0.1f).validate(true).mappers(5).resultingSplitsFilePath("/test").build();
        assertEquals(INPUT_DIRECTORY, sampleDataForSplitPoints.getInputPaths().get(0));
        assertEquals("true", sampleDataForSplitPoints.getOption(TEST_OPTION_KEY));
        assertEquals("/test", sampleDataForSplitPoints.getResultingSplitsFilePath());
        assertTrue(sampleDataForSplitPoints.isValidate());
        assertEquals(0.1f, sampleDataForSplitPoints.getProportionToSample(), 1);
        assertEquals(new Integer(5), sampleDataForSplitPoints.getNumMapTasks());
        assertEquals(new Integer(10), sampleDataForSplitPoints.getNumReduceTasks());
    }
}
