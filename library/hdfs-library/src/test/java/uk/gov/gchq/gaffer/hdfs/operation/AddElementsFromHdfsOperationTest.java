/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.hdfs.operation;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class AddElementsFromHdfsOperationTest implements OperationTest {

    private static final JSONSerialiser serialiser = new JSONSerialiser();
    public static final String ADD_ELEMENTS_FROM_HDFS_JSON = String.format("{%n" +
            "  \"inputPaths\" : [ \"TestInput\" ],%n" +
            "  \"outputPath\" : \"TestOutput\",%n" +
            "  \"validate\" : true%n" +
            "}");

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final AddElementsFromHdfs addElements = new AddElementsFromHdfs();

        // When
        String json = new String(serialiser.serialise(addElements, true));

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"inputPaths\" : [ ],%n" +
                "  \"validate\" : true%n" +
                "}"), json);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        AddElementsFromHdfs addElements = new AddElementsFromHdfs.Builder().option("testOption", "true").validate(true).addInputPath("input").failurePath("fail").mappers(10).reducers(20).outputPath("output").build();
        assertEquals("true", addElements.getOption("testOption"));
        assertTrue(addElements.isValidate());
        assertEquals("fail", addElements.getFailurePath());
        assertEquals(new Integer(10), addElements.getNumMapTasks());
        assertEquals(new Integer(20), addElements.getNumReduceTasks());
        assertEquals("output", addElements.getOutputPath());
        assertEquals("input", addElements.getInputPaths().get(0));
    }

    @Test
    public void shouldSerialisePopulatedAddElementsFromHdfsOperation() throws IOException {
        // Given
        final AddElementsFromHdfs addElementsFromHdfs = new AddElementsFromHdfs();
        addElementsFromHdfs.setInputPaths(Arrays.asList("TestInput"));
        addElementsFromHdfs.setOutputPath("TestOutput");
        // When
        String json = new String(serialiser.serialise(addElementsFromHdfs, true));
        // Then
        JsonUtil.assertEquals(ADD_ELEMENTS_FROM_HDFS_JSON, json);
    }

    @Test
    public void shouldDeserialiseAddElementsOperation() throws IOException {
        // When
        AddElementsFromHdfs addElementsFromHdfs = serialiser.deserialise(ADD_ELEMENTS_FROM_HDFS_JSON.getBytes(), AddElementsFromHdfs.class);

        // Then
        List<String> inputPaths = addElementsFromHdfs.getInputPaths();
        assertEquals("TestInput", inputPaths.get(0));
        assertEquals("TestOutput", addElementsFromHdfs.getOutputPath());

    }
}
