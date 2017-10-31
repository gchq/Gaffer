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

package uk.gov.gchq.gaffer.store.operation.handler;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.predicate.Exists;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class GetTraitsHandlerTest {

    public static final String STORE_ID = "StoreId";
    public static final String STRING = "string";
    private Store store;
    private HashSet<StoreTrait> storeTraits;
    private Schema string;

    @Before
    public void setUp() throws Exception {
        storeTraits = Sets.newHashSet(StoreTrait.ALL_TRAITS);
        storeTraits.remove(StoreTrait.ORDERED);

        store = new TestAddToGraphLibraryImpl() {
            @Override
            public Set<StoreTrait> getTraits() {
                return Sets.newHashSet(storeTraits);
            }
        };
        assertNotEquals(StoreTrait.ALL_TRAITS, storeTraits);
        string = new Schema.Builder().type(STRING, String.class).build();
    }

    @After
    public void tearDown() throws Exception {
        HashSet<StoreTrait> temp = Sets.newHashSet(StoreTrait.ALL_TRAITS);
        temp.remove(StoreTrait.ORDERED);
        assertEquals(temp, this.storeTraits);
        assertNotEquals(StoreTrait.ALL_TRAITS, storeTraits);
    }

    @Test
    public void shouldGetTraitsForSchemaEmpty() throws Exception {
        HashSet<StoreTrait> actual = getStoreTraits(new Schema());

        HashSet<StoreTrait> expected = Sets.newHashSet(this.storeTraits);
        expected.remove(StoreTrait.QUERY_AGGREGATION);
        expected.remove(StoreTrait.STORE_VALIDATION);
        expected.remove(StoreTrait.VISIBILITY);
        expected.remove(StoreTrait.INGEST_AGGREGATION);

        assertEquals(expected, actual);
    }

    @Test
    public void shouldGetTraitsForSchemaWithGroupBy() throws Exception {
        HashSet<StoreTrait> actual = getStoreTraits(new Schema.Builder()
                .entity("e1", new SchemaEntityDefinition.Builder()
                        .groupBy("gb")
                        .vertex(STRING)
                        .build())
                .merge(string)
                .build());

        HashSet<StoreTrait> expected = Sets.newHashSet(this.storeTraits);
        expected.remove(StoreTrait.STORE_VALIDATION);
        expected.remove(StoreTrait.VISIBILITY);
        assertEquals(expected, actual);
    }


    @Test
    public void shouldGetTraitsForSchemaWithValidator() throws Exception {

        HashSet<StoreTrait> actual = getStoreTraits(new Schema.Builder()
                .entity("e1", new SchemaEntityDefinition.Builder()
                        .property("p1", STRING)
                        .validator(new ElementFilter.Builder()
                                .select("p1")
                                .execute(new Exists())
                                .build())
                        .aggregate(false)
                        .vertex(STRING)
                        .build())
                .merge(string)
                .build());

        HashSet<StoreTrait> expected = Sets.newHashSet(this.storeTraits);
        expected.remove(StoreTrait.QUERY_AGGREGATION);
        expected.remove(StoreTrait.VISIBILITY);
        expected.remove(StoreTrait.INGEST_AGGREGATION);

        assertEquals(expected, actual);
    }


    @Test
    public void shouldGetTraitsForSchemaWithVisibility() throws Exception {
        HashSet<StoreTrait> actual = getStoreTraits(new Schema.Builder()
                .visibilityProperty(STRING)
                .build());

        HashSet<StoreTrait> expected = Sets.newHashSet(this.storeTraits);
        expected.remove(StoreTrait.QUERY_AGGREGATION);
        expected.remove(StoreTrait.STORE_VALIDATION);
        expected.remove(StoreTrait.INGEST_AGGREGATION);

        assertEquals(expected, actual);
    }

    @Test
    public void shouldGetTraitsForSchemaWithAggregatorAndGroupBy() throws Exception {
        HashSet<StoreTrait> actual = getStoreTraits(new Schema.Builder()
                .entity("e1", new SchemaEntityDefinition.Builder()
                        .property("p1", STRING)
                        .vertex(STRING)
                        .groupBy("p1")
                        .aggregator(new ElementAggregator.Builder()
                                .select("p1")
                                .execute(new StringConcat())
                                .build())
                        .build())
                .merge(string)
                .build());

        HashSet<StoreTrait> expected = Sets.newHashSet(this.storeTraits);
        expected.remove(StoreTrait.STORE_VALIDATION);
        expected.remove(StoreTrait.VISIBILITY);

        assertEquals(expected, actual);
    }

    private HashSet<StoreTrait> getStoreTraits(final Schema schema) throws StoreException, uk.gov.gchq.gaffer.operation.OperationException {
        store.initialise(STORE_ID, schema, new StoreProperties());
        Iterable<? extends StoreTrait> execute = store.execute(new GetTraits.Builder()
                .build(), new Context(testUser()));

        assertNotNull(execute);
        Iterator<? extends StoreTrait> iterator = execute.iterator();
        assertTrue(iterator.hasNext());


        HashSet<StoreTrait> actual = Sets.newHashSet();
        while (iterator.hasNext()) {
            actual.add(iterator.next());
        }
        return actual;
    }

    @Test
    public void shouldHaveAllTraitsForSupported() throws Exception {
        store.initialise(STORE_ID, new Schema(), new StoreProperties());
        Iterable<? extends StoreTrait> execute = store.execute(new GetTraits.Builder()
                .currentlyAvailableTraits(false)
                .build(), new Context(testUser()));

        assertNotNull(execute);
        Iterator<? extends StoreTrait> iterator = execute.iterator();
        assertTrue(iterator.hasNext());


        HashSet<StoreTrait> actual = Sets.newHashSet();
        while (iterator.hasNext()) {
            actual.add(iterator.next());
        }


        assertEquals(storeTraits, actual);
    }
}