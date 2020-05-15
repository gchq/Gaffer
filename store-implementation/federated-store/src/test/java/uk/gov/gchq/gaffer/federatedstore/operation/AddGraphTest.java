/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph.Builder;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AddGraphTest extends OperationTest<AddGraph> {

    private static final String EXPECTED_GRAPH_ID = "testGraphID";

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("graphId");
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        Schema expectedSchema = new Schema.Builder().build();
        StoreProperties storeProperties = new AccumuloProperties();
        AddGraph op = new AddGraph.Builder()
                .graphId(EXPECTED_GRAPH_ID)
                .schema(expectedSchema)
                .storeProperties(storeProperties)
                .build();

        assertEquals(EXPECTED_GRAPH_ID, op.getGraphId());
        assertEquals(expectedSchema, op.getSchema());
        assertNotNull(op.getStoreProperties().getStorePropertiesClassName());
        assertEquals(AccumuloProperties.class, op.getStoreProperties().getStorePropertiesClass());
    }

    @Override
    public void shouldShallowCloneOperation() {
        final AddGraph a = new Builder()
                .graphId("graphId")
                .parentPropertiesId("testPropID")
                .parentSchemaIds(Lists.newArrayList("testSchemaID"))
                .schema(new Schema.Builder()
                        .build())
                .graphAuths("testAuth")
                .storeProperties(new StoreProperties())
                .build();

        final AddGraph b = a.shallowClone();

        assertEquals(a.getGraphId(), b.getGraphId());
        assertEquals(a.getStoreProperties(), b.getStoreProperties());
        assertEquals(a.getSchema(), b.getSchema());
        assertEquals(a.getGraphAuths(), b.getGraphAuths());
    }

    @Test
    public void shouldShallowCloneOperationWithNulls() {
        final AddGraph a = new Builder()
                .graphId(null)
                .parentPropertiesId(null)
                .parentSchemaIds(null)
                .schema(null)
                .graphAuths(null)
                .storeProperties(null)
                .build();

        final AddGraph b = a.shallowClone();

        assertEquals(a.getGraphId(), b.getGraphId());
        assertEquals(a.getStoreProperties(), b.getStoreProperties());
        assertEquals(a.getSchema(), b.getSchema());
        assertEquals(a.getGraphAuths(), b.getGraphAuths());
    }

    @Override
    protected AddGraph getTestObject() {
        return new AddGraph();
    }
}
