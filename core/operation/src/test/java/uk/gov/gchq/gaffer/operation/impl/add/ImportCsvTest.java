/*
 * Copyright 2022 Crown Copyright
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
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;


class ImportCsvTest extends OperationTest<ImportCsv> {
    private static final String FILE_NAME = "filename";
    private static final char DELIMITER = ',';
    private static final String NULL_STRING = "";
    private static final Boolean TRIM = true;
    private static final boolean VALIDATE = true;
    private static final boolean SKIP_INVALID_ELEMENTS = false;
    private static final Map<String, String> OPTIONS = new HashMap<String, String>();


    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException, JsonProcessingException {
        // Given
        final ImportCsv op = new ImportCsv.Builder()
                .filename(FILE_NAME)
                .delimiter(DELIMITER)
                .trim(TRIM)
                .nullString(NULL_STRING)
                .validate(VALIDATE)
                .skipInvalidElements(SKIP_INVALID_ELEMENTS)
                .build();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);
        final ImportCsv deserialisedOp = JSONSerialiser.deserialise(json, ImportCsv.class);

        // Then
        JsonAssert.assertEquals(String.format("{%n" +
                        "  \"class\" : \"uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromOpenCypherCsv\",%n" +
                        "  \"filename\" : \"filename\",%n" +
                        "  \"delimiter\" : \",\",%n" +
                        "  \"nullString\" : \"\",%n" +
                        "  \"skipInvalidElements\" : false,%n" +
                        "  \"trim\" : true,%n" +
                        "  \"validate\" : true %n" +
                        "}").getBytes(),
                json);
        assertNotNull(deserialisedOp);
        assertThat(op)
                .usingRecursiveComparison()
                .isEqualTo(deserialisedOp);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final ImportCsv op = new ImportCsv.Builder()
                .filename(FILE_NAME)
                .delimiter(DELIMITER)
                .trim(TRIM)
                .nullString(NULL_STRING)
                .build();

        // Then
        assertEquals(FILE_NAME, op.getFilename());
        assertEquals(DELIMITER, op.getDelimiter());
        assertEquals(TRIM, op.isTrim());
        assertEquals(NULL_STRING, op.getNullString());
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final ImportCsv op = new ImportCsv.Builder()
                .filename(FILE_NAME)
                .validate(VALIDATE)
                .skipInvalidElements(SKIP_INVALID_ELEMENTS)
                .options(OPTIONS)
                .build();

        // When
        final ImportCsv clone = op.shallowClone();

        // Then
        assertNotSame(op, clone);
        assertThat(op)
                .usingRecursiveComparison()
                .isEqualTo(clone);
    }

    @Override
    public Set<String> getRequiredFields() {
        return Sets.newHashSet(FILE_NAME);
    }

    @Override
    protected ImportCsv getTestObject() {
        return new ImportCsv();
    }
}
