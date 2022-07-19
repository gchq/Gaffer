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
import com.google.common.collect.Sets;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.ArrayList;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class ToOpenCypherCsvTest extends OperationTest<ToOpenCypherCsv> {

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException, JsonProcessingException {
        // Given
        final ToOpenCypherCsv op = new ToOpenCypherCsv.Builder().build();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);
        final ToOpenCypherCsv deserialisedOp = JSONSerialiser.deserialise(json, ToOpenCypherCsv.class);

        // Then
        assertNotNull(deserialisedOp);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final Iterable<Element> input = new ArrayList<Element>() {
            {
                add(new Entity(TestGroups.ENTITY));
                add(new Entity(TestGroups.ENTITY_2));
                add(new Entity(TestGroups.EDGE));
            }
        };
        final ToOpenCypherCsv toOpenCypherCsv = new ToOpenCypherCsv.Builder()
                .input(input)
                .neo4jFormat(false)
                .build();

        // Then
        assertThat(toOpenCypherCsv.getInput())
                .hasSize(3);
        assertFalse(toOpenCypherCsv.isNeo4jFormat());
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Iterable<Element> input = new ArrayList<Element>() {
            {
                add(new Entity(TestGroups.ENTITY));
                add(new Entity(TestGroups.ENTITY_2));
                add(new Entity(TestGroups.EDGE));
            }
        };
        final ToOpenCypherCsv toOpenCypherCsv = new ToOpenCypherCsv.Builder()
                .input(input)
                .neo4jFormat(false)
                .build();


        // When
        final ToOpenCypherCsv clone = toOpenCypherCsv.shallowClone();

        // Then
        assertNotSame(toOpenCypherCsv, clone);
        assertThat(clone.getInput().equals(input));
        assertFalse(clone.isNeo4jFormat());
    }

    @Override
    public Set<String> getRequiredFields() {
        return Sets.newHashSet();
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertEquals(Iterable.class, outputClass);
    }

    @Override
    protected ToOpenCypherCsv getTestObject() {
        return new ToOpenCypherCsv();
    }
}
