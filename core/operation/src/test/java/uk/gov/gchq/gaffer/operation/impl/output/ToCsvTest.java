/*
 * Copyright 2017-2023 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.CsvGenerator;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class ToCsvTest extends OperationTest<ToCsv> {

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException, JsonProcessingException {
        // Given
        final ToCsv op = new ToCsv.Builder().build();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);
        final ToCsv deserialisedOp = JSONSerialiser.deserialise(json, ToCsv.class);

        // Then
        assertNotNull(deserialisedOp);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final Entity input = new Entity(TestGroups.ENTITY);
        final CsvGenerator generator = new CsvGenerator.Builder().group("group").build();
        final ToCsv toCsv = new ToCsv.Builder()
                .generator(generator)
                .input(input)
                .includeHeader(false)
                .build();

        // Then
        assertThat(toCsv.getInput())
                .hasSize(1);
        assertFalse(toCsv.isIncludeHeader());
        assertEquals(generator, toCsv.getCsvGenerator());
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Entity input = new Entity(TestGroups.ENTITY);
        final CsvGenerator generator = new CsvGenerator.Builder().group("group").build();
        final ToCsv toCsv = new ToCsv.Builder()
                .generator(generator)
                .input(input)
                .includeHeader(false)
                .build();

        // When
        final ToCsv clone = toCsv.shallowClone();

        // Then
        assertNotSame(toCsv, clone);
        assertThat(clone.getInput().iterator().next()).isEqualTo(input);
        assertEquals(generator, clone.getCsvGenerator());
        assertFalse(clone.isIncludeHeader());
    }
    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertEquals(Iterable.class, outputClass);
    }

    @Override
    protected ToCsv getTestObject() {
        return new ToCsv();
    }
}
