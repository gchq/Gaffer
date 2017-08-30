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

package uk.gov.gchq.gaffer.federatedstore.operation;

import com.google.common.collect.Sets;
import org.junit.Assert;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.Set;

public class AddGraphTest extends OperationTest<AddGraph> {

    public static final String expectedKey = "gaffer.store.class";

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("graphId");
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        Schema expectedSchema = new Schema.Builder().build();
        String expectedGraphId = "testGraphID";
        StoreProperties storeProperties = new StoreProperties();
        String expectedValue = "uk.gov.gchq.gaffer.federatedstore.FederatedStore";
        storeProperties.set(expectedKey, expectedValue);
        AddGraph op = new AddGraph.Builder()
                .setGraphId(expectedGraphId)
                .schema(expectedSchema)
                .storeProperties(storeProperties)
                .build();

        Assert.assertEquals(expectedGraphId, op.getGraphId());
        Assert.assertEquals(expectedSchema, op.getSchema());
        Assert.assertTrue(op.getStoreProperties().containsKey(expectedKey));
        Assert.assertEquals(expectedValue, op.getStoreProperties().get(expectedKey));
    }

    @Override
    public void shouldShallowCloneOperation() {
        final AddGraph a = getTestObject();
        final AddGraph b = a.shallowClone();

        Assert.assertEquals(a.getGraphId(), b.getGraphId());
        Assert.assertEquals(a.getStoreProperties(), b.getStoreProperties());
        Assert.assertEquals(a.getSchema(), b.getSchema());
    }

    @Override
    protected AddGraph getTestObject() {
        return new AddGraph();
    }
}