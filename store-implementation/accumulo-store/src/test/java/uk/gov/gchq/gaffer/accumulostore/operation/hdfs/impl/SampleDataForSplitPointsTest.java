package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.impl;


import com.google.common.collect.Sets;
import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SampleDataForSplitPointsTest extends OperationTest<SampleDataForSplitPoints> {
    private static final JSONSerialiser SERIALISER = new JSONSerialiser();
    private static final String INPUT_DIRECTORY = "/input";
    private static final String TEST_OPTION_KEY = "testOption";

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet(
                "splitsFilePath",
                "mapperGeneratorClassName",
                "inputPaths",
                "outputPath",
                "jobInitialiser"
        );
    }

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final String resultPath = "/result";
        final SampleDataForSplitPoints op = new SampleDataForSplitPoints();
        op.setInputPaths(Arrays.asList(INPUT_DIRECTORY));
        op.setMapperGeneratorClassName("Test");
        op.setValidate(true);
        op.setProportionToSample(0.1f);
        op.setSplitsFilePath(resultPath);
        op.setNumMapTasks(5);

        // When
        byte[] json = SERIALISER.serialise(op, true);
        final SampleDataForSplitPoints deserialisedOp = SERIALISER.deserialise(json, SampleDataForSplitPoints.class);

        // Then
        assertEquals(INPUT_DIRECTORY, deserialisedOp.getInputPaths().get(0));
        assertEquals(resultPath, deserialisedOp.getSplitsFilePath());
        assertEquals("Test", deserialisedOp.getMapperGeneratorClassName());
        assertTrue(deserialisedOp.isValidate());
        assertEquals(0.1f, deserialisedOp.getProportionToSample(), 1);
        assertEquals(new Integer(5), deserialisedOp.getNumMapTasks());
        assertEquals(new Integer(1), deserialisedOp.getNumReduceTasks());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final SampleDataForSplitPoints sampleDataForSplitPoints = new SampleDataForSplitPoints.Builder()
                .addInputPath(INPUT_DIRECTORY)
                .splitsFilePath("/test")
                .proportionToSample(0.1f)
                .mappers(5)
                .validate(true)
                .option(TEST_OPTION_KEY, "true")
                .build();
        assertEquals(INPUT_DIRECTORY, sampleDataForSplitPoints.getInputPaths().get(0));
        assertEquals("true", sampleDataForSplitPoints.getOption(TEST_OPTION_KEY));
        assertEquals("/test", sampleDataForSplitPoints.getSplitsFilePath());
        assertTrue(sampleDataForSplitPoints.isValidate());
        assertEquals(0.1f, sampleDataForSplitPoints.getProportionToSample(), 1);
        assertEquals(new Integer(5), sampleDataForSplitPoints.getNumMapTasks());
    }

    @Test
    public void expectIllegalArgumentExceptionWhenTryingToSetReducers() {
        final SampleDataForSplitPoints op = getTestObject();
        try {
            op.setNumReduceTasks(10);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Override
    protected SampleDataForSplitPoints getTestObject() {
        return new SampleDataForSplitPoints();
    }
}
