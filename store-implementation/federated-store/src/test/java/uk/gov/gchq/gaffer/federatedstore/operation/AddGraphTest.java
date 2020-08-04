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
import org.junit.Assert;
import org.junit.Test;

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.CustomAccessPredicate;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph.Builder;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;

public class AddGraphTest extends OperationTest<AddGraph> {

    private static final String EXPECTED_GRAPH_ID = "testGraphID";
    private static final AccessPredicate READ_ACCESS_PREDICATE = new CustomAccessPredicate("creatorUserId", emptyMap(), asList("readAuth1", "readAuth2"));
    private static final AccessPredicate WRITE_ACCESS_PREDICATE = new CustomAccessPredicate("creatorUserId", emptyMap(), asList("writeAuth1", "writeAuth2"));

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
                .readAccessPredicate(READ_ACCESS_PREDICATE)
                .writeAccessPredicate(WRITE_ACCESS_PREDICATE)
                .build();

        Assert.assertEquals(EXPECTED_GRAPH_ID, op.getGraphId());
        Assert.assertEquals(expectedSchema, op.getSchema());
        Assert.assertNotNull(op.getStoreProperties().getStorePropertiesClassName());
        Assert.assertEquals(AccumuloProperties.class, op.getStoreProperties().getStorePropertiesClass());
        Assert.assertEquals(READ_ACCESS_PREDICATE, op.getReadAccessPredicate());
        Assert.assertEquals(WRITE_ACCESS_PREDICATE, op.getWriteAccessPredicate());
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
                .readAccessPredicate(READ_ACCESS_PREDICATE)
                .writeAccessPredicate(WRITE_ACCESS_PREDICATE)
                .build();

        final AddGraph b = a.shallowClone();

        Assert.assertEquals(a.getGraphId(), b.getGraphId());
        Assert.assertEquals(a.getStoreProperties(), b.getStoreProperties());
        Assert.assertEquals(a.getSchema(), b.getSchema());
        Assert.assertEquals(a.getGraphAuths(), b.getGraphAuths());
        Assert.assertEquals(a.getReadAccessPredicate(), b.getReadAccessPredicate());
        Assert.assertEquals(a.getWriteAccessPredicate(), b.getWriteAccessPredicate());
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
                .readAccessPredicate(READ_ACCESS_PREDICATE)
                .writeAccessPredicate(WRITE_ACCESS_PREDICATE)
                .build();

        final AddGraph b = a.shallowClone();

        Assert.assertEquals(a.getGraphId(), b.getGraphId());
        Assert.assertEquals(a.getStoreProperties(), b.getStoreProperties());
        Assert.assertEquals(a.getSchema(), b.getSchema());
        Assert.assertEquals(a.getGraphAuths(), b.getGraphAuths());
        Assert.assertEquals(a.getReadAccessPredicate(), b.getReadAccessPredicate());
        Assert.assertEquals(a.getWriteAccessPredicate(), b.getWriteAccessPredicate());
    }

    @Override
    protected AddGraph getTestObject() {
        return new AddGraph();
    }
}
