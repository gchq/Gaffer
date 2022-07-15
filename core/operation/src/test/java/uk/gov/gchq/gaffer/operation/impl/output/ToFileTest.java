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

package uk.gov.gchq.gaffer.operation.impl.output;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class ToFileTest extends OperationTest<ToFile> {

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException, JsonProcessingException {
        // Given
        final ToFile op = new ToFile.Builder().build();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);
        final ToFile deserialisedOp = JSONSerialiser.deserialise(json, ToFile.class);

        // Then
        assertNotNull(deserialisedOp);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final String filePath = "filePath";
        final Iterable<String> input = new ArrayList<>(
                Arrays.asList("header",
                              "line1",
                              "line2"));
        final Map<String, String> options = new HashMap<>();
        final ToFile toFile = new ToFile.Builder()
                .filePath(filePath)
                .input(input)
                .options(options)
                .build();

        // Then
        assertThat(toFile.getInput())
                .hasSize(3);
        assertThat(toFile.getOptions().isEmpty());
        assertThat(toFile.getFilePath().contentEquals(filePath));
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final String filePath = "filePath";
        final Iterable<String> input = new ArrayList<>(
                Arrays.asList("header",
                        "line1",
                        "line2"));
        final Map<String, String> options = new HashMap<>();
        final ToFile toFile = new ToFile.Builder()
                .filePath(filePath)
                .input(input)
                .options(options)
                .build();

        // When
        final ToFile clone = toFile.shallowClone();

        // Then
        assertNotSame(toFile, clone);
        assertThat(clone.getInput().toString().contentEquals(input.toString()));
        assertThat(clone.getFilePath()).isEqualTo(filePath);
        assertThat(clone.getOptions()).isEqualTo(options);
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertEquals(File.class, outputClass);
    }

    @Override
    protected ToFile getTestObject() {
        return new ToFile();
    }
}
