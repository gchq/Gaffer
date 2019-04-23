/*
 * Copyright 2016-2019 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.impl;

import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.MapperGenerator;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SampleDataForSplitPointsTest extends OperationTest<SampleDataForSplitPoints> {
    private static final String INPUT_DIRECTORY = "/input";
    private static final String TEST_OPTION_KEY = "testOption";

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet(
                "jobInitialiser",
                "outputPath",
                "splitsFilePath",
                "inputMapperPairs"
        );
    }

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final Map<String, String> inputMapperPairs = new HashMap<>();
        inputMapperPairs.put(INPUT_DIRECTORY, MapperGenerator.class.getName());
        final String resultPath = "/result";
        final SampleDataForSplitPoints op = new SampleDataForSplitPoints();
        op.setInputMapperPairs(inputMapperPairs);
        op.setValidate(true);
        op.setProportionToSample(0.1f);
        op.setSplitsFilePath(resultPath);
        op.setNumMapTasks(5);

        // When
        byte[] json = JSONSerialiser.serialise(op, true);
        final SampleDataForSplitPoints deserialisedOp = JSONSerialiser.deserialise(json, SampleDataForSplitPoints.class);

        // Then
        assertEquals(MapperGenerator.class.getName(), deserialisedOp.getInputMapperPairs().get(INPUT_DIRECTORY));
        assertEquals(resultPath, deserialisedOp.getSplitsFilePath());
        assertTrue(deserialisedOp.isValidate());
        assertEquals(0.1f, deserialisedOp.getProportionToSample(), 1);
        assertEquals(new Integer(5), deserialisedOp.getNumMapTasks());
        assertEquals(new Integer(1), deserialisedOp.getNumReduceTasks());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final SampleDataForSplitPoints sampleDataForSplitPoints = new SampleDataForSplitPoints.Builder()
                .addInputMapperPair(INPUT_DIRECTORY, MapperGenerator.class.getName())
                .splitsFilePath("/test")
                .proportionToSample(0.1f)
                .mappers(5)
                .validate(true)
                .option(TEST_OPTION_KEY, "true")
                .build();
        assertEquals(MapperGenerator.class.getName(), sampleDataForSplitPoints.getInputMapperPairs().get(INPUT_DIRECTORY));
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
    public void shouldShallowCloneOperation() {
        // Given
        final SampleDataForSplitPoints sampleDataForSplitPoints = new SampleDataForSplitPoints.Builder()
                .addInputMapperPair(INPUT_DIRECTORY, MapperGenerator.class.getName())
                .splitsFilePath("/test")
                .proportionToSample(0.1f)
                .mappers(5)
                .validate(true)
                .option(TEST_OPTION_KEY, "true")
                .build();

        //When
        SampleDataForSplitPoints clone = sampleDataForSplitPoints.shallowClone();

        // Then
        assertNotSame(sampleDataForSplitPoints, clone);
        assertEquals(MapperGenerator.class.getName(), clone.getInputMapperPairs().get(INPUT_DIRECTORY));
        assertEquals("true", clone.getOption(TEST_OPTION_KEY));
        assertEquals("/test", clone.getSplitsFilePath());
        assertTrue(clone.isValidate());
        assertEquals(0.1f, clone.getProportionToSample(), 1);
        assertEquals(new Integer(5), clone.getNumMapTasks());
    }

    @Test
    public void shouldShallowCloneOperationWithMinAndMaxMappers() {
        // Given
        final SampleDataForSplitPoints sampleDataForSplitPoints = new SampleDataForSplitPoints.Builder()
                .addInputMapperPair(INPUT_DIRECTORY, MapperGenerator.class.getName())
                .splitsFilePath("/test")
                .proportionToSample(0.1f)
                .maxMappers(10)
                .minMappers(2)
                .validate(true)
                .option(TEST_OPTION_KEY, "true")
                .build();

        //When
        SampleDataForSplitPoints clone = sampleDataForSplitPoints.shallowClone();

        // Then
        assertEquals(MapperGenerator.class.getName(), clone.getInputMapperPairs().get(INPUT_DIRECTORY));
        assertEquals("true", clone.getOption(TEST_OPTION_KEY));
        assertEquals("/test", clone.getSplitsFilePath());
        assertTrue(clone.isValidate());
        assertEquals(0.1f, clone.getProportionToSample(), 1);
        assertEquals(new Integer(10), clone.getMaxMapTasks());
        assertEquals(new Integer(2), clone.getMinMapTasks());
    }

    @Override
    protected SampleDataForSplitPoints getTestObject() {
        return new SampleDataForSplitPoints();
    }
}
