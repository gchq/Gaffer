/*
 * Copyright 2017-2022 Crown Copyright
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

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.user.CustomUserPredicate;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraphWithHooks.Builder;
import uk.gov.gchq.gaffer.graph.hook.Log4jLogger;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AddGraphWithHooksTest extends FederationOperationTest<AddGraphWithHooks> {

    private static final String EXPECTED_GRAPH_ID = "testGraphID";
    private static final AccessPredicate READ_ACCESS_PREDICATE = new AccessPredicate(new CustomUserPredicate());
    private static final AccessPredicate WRITE_ACCESS_PREDICATE = new AccessPredicate(new CustomUserPredicate());

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("graphId");
    }

    @Test
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
                .readAccessPredicate(READ_ACCESS_PREDICATE)
                .writeAccessPredicate(WRITE_ACCESS_PREDICATE)
                .build();

        assertEquals(EXPECTED_GRAPH_ID, op.getGraphId());
        assertEquals(expectedSchema, op.getSchema());
        assertNotNull(op.getStoreProperties().getStorePropertiesClassName());
        assertEquals(AccumuloProperties.class, op.getStoreProperties().getStorePropertiesClass());
        assertThat(op.getHooks()).hasSize(1);
        assertEquals(log4jLogger, op.getHooks()[0]);
        assertEquals(READ_ACCESS_PREDICATE, op.getReadAccessPredicate());
        assertEquals(WRITE_ACCESS_PREDICATE, op.getWriteAccessPredicate());
    }

    @Test
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
                .disabledByDefault(true)
                .readAccessPredicate(READ_ACCESS_PREDICATE)
                .writeAccessPredicate(WRITE_ACCESS_PREDICATE)
                .build();

        final AddGraphWithHooks b = a.shallowClone();

        assertEquals(a.getGraphId(), b.getGraphId());
        assertEquals(a.getStoreProperties(), b.getStoreProperties());
        assertEquals(a.getSchema(), b.getSchema());
        assertEquals(a.getGraphAuths(), b.getGraphAuths());
        assertArrayEquals(a.getHooks(), b.getHooks());
        assertTrue(b.isDisabledByDefault());
        assertEquals(a.getReadAccessPredicate(), b.getReadAccessPredicate());
        assertEquals(a.getWriteAccessPredicate(), b.getWriteAccessPredicate());
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
                .readAccessPredicate(READ_ACCESS_PREDICATE)
                .writeAccessPredicate(WRITE_ACCESS_PREDICATE)
                .build();

        final AddGraphWithHooks b = a.shallowClone();

        assertEquals(a.getGraphId(), b.getGraphId());
        assertEquals(a.getStoreProperties(), b.getStoreProperties());
        assertEquals(a.getSchema(), b.getSchema());
        assertEquals(a.getGraphAuths(), b.getGraphAuths());
        assertEquals(a.getReadAccessPredicate(), b.getReadAccessPredicate());
        assertEquals(a.getWriteAccessPredicate(), b.getWriteAccessPredicate());
    }

    @Override
    protected AddGraphWithHooks getTestObject() {
        return new AddGraphWithHooks();
    }
}
