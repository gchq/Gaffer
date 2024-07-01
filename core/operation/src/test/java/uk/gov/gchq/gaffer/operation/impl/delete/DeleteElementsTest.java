/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.delete;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class DeleteElementsTest extends OperationTest<DeleteElements> {

    public static final String DELETE_ELEMENTS_JSON = String.format("{%n" +
            "  \"class\" : \"uk.gov.gchq.gaffer.operation.impl.delete.DeleteElements\",%n" +
            "  \"skipInvalidElements\" : false,%n" +
            "  \"validate\" : true%n" +
            "}");

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final DeleteElements deleteElements = new DeleteElements.Builder()
                .validate(false)
                .skipInvalidElements(false)
                .option("testOption", "true")
                .build();

        // When
        final DeleteElements clone = deleteElements.shallowClone();

        // Then
        assertThat(clone).isNotSameAs(deleteElements);
        assertThat(clone.isValidate()).isFalse();
        assertThat(clone.isSkipInvalidElements()).isFalse();
        assertThat(clone.getOption("testOption")).isEqualTo("true");
    }

    @Test
    void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final DeleteElements deleteElements = getTestObject();

        final Map<String, String> options = new HashMap<>();
        options.put("option", "value");

        deleteElements.setOptions(options);

        // When
        String json = new String(JSONSerialiser.serialise(deleteElements, true));

        // Then
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.operation.impl.delete.DeleteElements\",%n" +
                "  \"validate\" : true,%n" +
                "  \"options\" : {\"option\": \"value\"},%n" +
                "  \"skipInvalidElements\" : false%n" +
                "}"), json);
    }

    @Test
    void shouldSerialiseDeleteElementsOperation() throws IOException {
        // Given
         final DeleteElements deleteElements = new DeleteElements.Builder().build();

        // When
        String json = new String(JSONSerialiser.serialise(deleteElements, true));

        // Then
        JsonAssert.assertEquals(DELETE_ELEMENTS_JSON, json);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given/When
        final DeleteElements deleteElements = new DeleteElements.Builder()
                .skipInvalidElements(true)
                .option("testOption", "true")
                .validate(false)
                .build();

        // Then
        assertThat(deleteElements.getOption("testOption")).isEqualTo("true");
        assertThat(deleteElements.isSkipInvalidElements()).isTrue();
        assertThat(deleteElements.isValidate()).isFalse();
    }

    @Test
    void shouldGetDeleteElementsObjectToString() {
        // Given
        final DeleteElements deleteElements = getTestObject();

        // When/Then
        assertThat(deleteElements).hasToString("DeleteElements[validate=true,skipInvalidElements=false]");
    }

    @Override
    protected DeleteElements getTestObject() {
        return new DeleteElements();
    }
}
