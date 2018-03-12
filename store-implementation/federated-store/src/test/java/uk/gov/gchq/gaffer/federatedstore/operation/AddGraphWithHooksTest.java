/*
 * Copyright 2018 Crown Copyright
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
import org.junit.Assert;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraphWithHooks.Builder;
import uk.gov.gchq.gaffer.graph.hook.Log4jLogger;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Set;

public class AddGraphWithHooksTest extends OperationTest<AddGraphWithHooks> {

    private static final String EXPECTED_GRAPH_ID = "testGraphID";

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("graphId");
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        Schema expectedSchema = new Schema.Builder().build();
        StoreProperties storeProperties = new AccumuloProperties();
        Log4jLogger log4jLogger = new Log4jLogger();

        AddGraphWithHooks op = new Builder()
                .graphId(EXPECTED_GRAPH_ID)
                .schema(expectedSchema)
                .storeProperties(storeProperties)
                .hooks(log4jLogger)
                .build();

        Assert.assertEquals(EXPECTED_GRAPH_ID, op.getGraphId());
        Assert.assertEquals(expectedSchema, op.getSchema());
        Assert.assertNotNull(op.getStoreProperties().getStorePropertiesClassName());
        Assert.assertEquals(AccumuloProperties.class, op.getStoreProperties().getStorePropertiesClass());
        Assert.assertEquals(1, op.getHooks().length);
        Assert.assertEquals(log4jLogger, op.getHooks()[0]);
    }

    @Override
    public void shouldShallowCloneOperation() {
        final AddGraphWithHooks a = new Builder()
                .graphId("graphId")
                .parentPropertiesId("testPropID")
                .parentSchemaIds(Lists.newArrayList("testSchemaID"))
                .schema(new Schema.Builder()
                        .build())
                .graphAuths("testAuth")
                .storeProperties(new StoreProperties())
                .hooks(new Log4jLogger())
                .build();

        final AddGraphWithHooks b = a.shallowClone();

        Assert.assertEquals(a.getGraphId(), b.getGraphId());
        Assert.assertEquals(a.getStoreProperties(), b.getStoreProperties());
        Assert.assertEquals(a.getSchema(), b.getSchema());
        Assert.assertEquals(a.getGraphAuths(), b.getGraphAuths());
        Assert.assertArrayEquals(a.getHooks(), b.getHooks());
    }

    @Test
    public void shouldShallowCloneOperationWithNulls() {
        final AddGraphWithHooks a = new Builder()
                .graphId(null)
                .parentPropertiesId(null)
                .parentSchemaIds(null)
                .schema(null)
                .graphAuths(null)
                .storeProperties(null)
                .build();

        final AddGraphWithHooks b = a.shallowClone();

        Assert.assertEquals(a.getGraphId(), b.getGraphId());
        Assert.assertEquals(a.getStoreProperties(), b.getStoreProperties());
        Assert.assertEquals(a.getSchema(), b.getSchema());
        Assert.assertEquals(a.getGraphAuths(), b.getGraphAuths());
    }

    @Override
    protected AddGraphWithHooks getTestObject() {
        return new AddGraphWithHooks();
    }

}