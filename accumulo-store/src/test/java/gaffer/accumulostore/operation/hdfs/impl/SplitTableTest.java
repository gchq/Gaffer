package gaffer.accumulostore.operation.hdfs.impl;


import gaffer.accumulostore.operation.impl.GetEdgesBetweenSets;
import gaffer.data.elementdefinition.view.View;
import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.GetOperation;
import gaffer.operation.OperationTest;
import gaffer.operation.data.EntitySeed;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.*;

public class SplitTableTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        String inputDir = "/input";
        final SplitTable op = new SplitTable();
        op.setInputPath(inputDir);

        // When
        byte[] json = serialiser.serialise(op, true);

        final SplitTable deserialisedOp = serialiser.deserialise(json, SplitTable.class);

        // Then
        assertEquals(inputDir, deserialisedOp.getInputPath());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        String inputPath = "/input";
        SplitTable splitTable = new SplitTable.Builder().inputPath(inputPath).option("testOption", "true").build();
        assertEquals(inputPath, splitTable.getInputPath());
        assertEquals("true", splitTable.getOption("testOption"));
    }
}
