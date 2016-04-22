package gaffer.accumulostore.operation.hdfs.impl;


import gaffer.accumulostore.operation.impl.GetEdgesBetweenSets;
import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.OperationTest;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SampleDataForSplitPointsTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        String inputDir = "/input";
        String resultPath = "/result";
        final SampleDataForSplitPoints op = new SampleDataForSplitPoints();
        op.setInputPaths(Arrays.asList(inputDir));
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
        assertEquals(inputDir, deserialisedOp.getInputPaths().get(0));
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
        String inputPath = "/input";
        SampleDataForSplitPoints sampleDataForSplitPoints = new SampleDataForSplitPoints.Builder().addInputPath(inputPath).option("testOption", "true").reducers(10).proportionToSample(0.1f).validate(true).mappers(5).resultingSplitsFilePath("/test").build();
        assertEquals(inputPath, sampleDataForSplitPoints.getInputPaths().get(0));
        assertEquals("true", sampleDataForSplitPoints.getOption("testOption"));
        assertEquals("/test", sampleDataForSplitPoints.getResultingSplitsFilePath());
        assertTrue(sampleDataForSplitPoints.isValidate());
        assertEquals(0.1f, sampleDataForSplitPoints.getProportionToSample(), 1);
        assertEquals(new Integer(5), sampleDataForSplitPoints.getNumMapTasks());
        assertEquals(new Integer(10), sampleDataForSplitPoints.getNumReduceTasks());
    }
}
