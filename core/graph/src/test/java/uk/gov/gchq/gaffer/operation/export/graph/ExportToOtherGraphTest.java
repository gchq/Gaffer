/*
 * Copyright 2017 Crown Copyright
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
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class ExportToOtherGraphTest extends OperationTest<ExportToOtherGraph> {
    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException, JsonProcessingException {
        // Given
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition())
                .build();
        final StoreProperties storeProperties = StoreProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));
        final ExportToOtherGraph op = new ExportToOtherGraph.Builder()
                .graphId("graphId")
                .parentSchemaIds("schema1", "schema2")
                .parentStorePropertiesId("props1")
                .schema(schema)
                .storeProperties(storeProperties)
                .build();

        // When
        final byte[] json = JSON_SERIALISER.serialise(op);
        final ExportToOtherGraph deserialisedOp = JSON_SERIALISER.deserialise(json, op.getClass());

        // Then
        assertEquals("graphId", deserialisedOp.getGraphId());
        assertEquals(Arrays.asList("schema1", "schema2"), deserialisedOp.getParentSchemaIds());
        assertEquals("props1", deserialisedOp.getParentStorePropertiesId());
        JsonAssert.assertEquals(schema.toJson(false), deserialisedOp.getSchema().toJson(false));
        assertEquals(storeProperties, deserialisedOp.getStoreProperties());
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition())
                .build();
        final StoreProperties storeProperties = StoreProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));

        // When
        final ExportToOtherGraph op = new ExportToOtherGraph.Builder()
                .graphId("graphId")
                .parentSchemaIds("schema1", "schema2")
                .parentStorePropertiesId("props1")
                .schema(schema)
                .storeProperties(storeProperties)
                .build();

        // Then
        assertEquals("graphId", op.getGraphId());
        assertEquals(Arrays.asList("schema1", "schema2"), op.getParentSchemaIds());
        assertEquals("props1", op.getParentStorePropertiesId());
        JsonAssert.assertEquals(schema.toJson(false), op.getSchema().toJson(false));
        assertEquals(storeProperties, op.getStoreProperties());
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("graphId");
    }

    @Override
    protected ExportToOtherGraph getTestObject() {
        return new ExportToOtherGraph();
    }
}
