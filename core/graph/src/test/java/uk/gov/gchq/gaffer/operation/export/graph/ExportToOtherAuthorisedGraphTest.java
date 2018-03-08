/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.export.graph;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class ExportToOtherAuthorisedGraphTest extends OperationTest {

    private ExportToOtherAuthorisedGraph op = new ExportToOtherAuthorisedGraph.Builder()
            .graphId("graphId")
            .parentSchemaIds("schema1")
            .parentStorePropertiesId("props1")
            .build();

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException, JsonProcessingException {
        // Given / When
        final byte[] json = JSONSerialiser.serialise(op);
        final ExportToOtherAuthorisedGraph deserialisedOp = JSONSerialiser.deserialise(json, op.getClass());

        // Then
        assertEquals("graphId", deserialisedOp.getGraphId());
        assertEquals(Collections.singletonList("schema1"), deserialisedOp.getParentSchemaIds());
        assertEquals("props1", deserialisedOp.getParentStorePropertiesId());
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given / When / Then
        assertEquals("graphId", op.getGraphId());
        assertEquals(Collections.singletonList("schema1"), op.getParentSchemaIds());
        assertEquals("props1", op.getParentStorePropertiesId());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // When
        ExportToOtherAuthorisedGraph clone = op.shallowClone();

        // Then
        assertNotSame(op, clone);
        assertEquals("graphId", clone.getGraphId());
        assertEquals(Collections.singletonList("schema1"), clone.getParentSchemaIds());
        assertEquals("props1", clone.getParentStorePropertiesId());
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("graphId");
    }

    @Override
    protected ExportToOtherAuthorisedGraph getTestObject() {
        return new ExportToOtherAuthorisedGraph();
    }
}
