/*
 * Copyright 2017-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.add;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Sets;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class AddElementsFromCsvTest extends OperationTest<AddElementsFromCsv> {

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException, JsonProcessingException {
        // Given
        final String filename = "filename";
        final String mappingsFile = "mappingsFile.json";
        final boolean validate = true;
        final boolean skipInvalid = false;
        final AddElementsFromCsv op = new AddElementsFromCsv.Builder()
                .filename(filename)
                .mappingsFile(mappingsFile)
                .validate(validate)
                .skipInvalidElements(skipInvalid)
                .build();

        // When
        final byte[] json = JSONSerialiser.serialise(op, true);
        final AddElementsFromCsv deserialisedOp = JSONSerialiser.deserialise(json, AddElementsFromCsv.class);

        // Then
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromCsv\",%n" +
                "  \"filename\" : \"filename\",%n" +
                "  \"mappingsFile\" : \"mappingsFile.json\",%n" +
                "  \"skipInvalidElements\" : false,%n" +
                "  \"validate\" : true%n" +
                "}").getBytes(), json);
        assertEquals(filename, deserialisedOp.getFilename());
        assertEquals(mappingsFile, deserialisedOp.getMappingsFile());
        assertEquals(validate, deserialisedOp.isValidate());
        assertEquals(skipInvalid, deserialisedOp.isSkipInvalidElements());
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final String filename = "filename";
        final String mappingsFile = "mappingsFile.json";
        final boolean validate = true;
        final boolean skipInvalid = false;

        // When
        final AddElementsFromCsv op = new AddElementsFromCsv.Builder()
                .filename(filename)
                .mappingsFile(mappingsFile)
                .validate(validate)
                .skipInvalidElements(skipInvalid)
                .build();

        // Then
        assertEquals(filename, op.getFilename());
        assertEquals(mappingsFile, op.getMappingsFile());
        assertEquals(validate, op.isValidate());
        assertEquals(skipInvalid, op.isSkipInvalidElements());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final String filename = "filename";
        final String mappingsFile = "mappingsFile.json";
        final boolean validate = true;
        final boolean skipInvalid = false;
        final AddElementsFromCsv op = new AddElementsFromCsv.Builder()
                .filename(filename)
                .mappingsFile(mappingsFile)
                .validate(validate)
                .skipInvalidElements(skipInvalid)
                .option("testOption", "true")
                .build();

        // When
        AddElementsFromCsv clone = op.shallowClone();

        // Then
        assertNotSame(op, clone);
        assertEquals(validate, clone.isValidate());
        assertEquals(skipInvalid, clone.isSkipInvalidElements());
        assertEquals(filename, clone.getFilename());
        assertEquals(mappingsFile, clone.getMappingsFile());
        assertEquals("true", clone.getOption("testOption"));
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("filename", "mappingsFile");
    }

    @Override
    protected AddElementsFromCsv getTestObject() {
        return new AddElementsFromCsv();
    }
}
