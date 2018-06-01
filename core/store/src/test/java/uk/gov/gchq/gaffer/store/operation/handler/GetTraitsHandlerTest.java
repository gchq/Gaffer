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

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class GetTraitsHandlerTest {

    public static final String STORE_ID = "StoreId";
    public static final String STRING = "string";
    private Store store;
    private Set<StoreTrait> expectedTraits;
    private Schema string;

    @Before
    public void setUp() throws Exception {
        expectedTraits = Sets.newHashSet(StoreTrait.ALL_TRAITS);
        expectedTraits.remove(StoreTrait.ORDERED);

        store = new TestAddToGraphLibraryImpl() {
            @Override
            public Set<StoreTrait> getTraits() {
                return Sets.newHashSet(expectedTraits);
            }
        };
        assertNotEquals(StoreTrait.ALL_TRAITS, expectedTraits);
        string = new Schema.Builder().type(STRING, String.class).build();
    }

    @After
    public void tearDown() throws Exception {
        final Set<StoreTrait> temp = Sets.newHashSet(StoreTrait.ALL_TRAITS);
        temp.remove(StoreTrait.ORDERED);
        assertEquals(temp, this.expectedTraits);
        assertNotEquals(StoreTrait.ALL_TRAITS, expectedTraits);
    }

    @Test
    public void shouldGetTraitsForSchemaEmpty() throws Exception {
        // When
        final Set<StoreTrait> actual = getStoreTraits(new Schema());

        // Then
        final Set<StoreTrait> expected = Sets.newHashSet(this.expectedTraits);
        expected.remove(StoreTrait.QUERY_AGGREGATION);
        expected.remove(StoreTrait.STORE_VALIDATION);
        expected.remove(StoreTrait.VISIBILITY);
        expected.remove(StoreTrait.INGEST_AGGREGATION);
        assertEquals(expected, actual);
    }

    @Test
    public void shouldGetTraitsForSchemaWithGroupBy() throws Exception {
        // When
        final Set<StoreTrait> actual = getStoreTraits(new Schema.Builder()
                .entity("e1", new SchemaEntityDefinition.Builder()
                        .groupBy("gb")
                        .vertex(STRING)
                        .build())
                .merge(string)
                .build());

        final Set<StoreTrait> expected = Sets.newHashSet(this.expectedTraits);
        expected.remove(StoreTrait.STORE_VALIDATION);
        expected.remove(StoreTrait.VISIBILITY);
        assertEquals(expected, actual);
    }

    @Test
    public void shouldGetTraitsForSchemaWithValidator() throws Exception {
        // When
        final Set<StoreTrait> actual = getStoreTraits(new Schema.Builder()
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

        // Then
        final Set<StoreTrait> expected = Sets.newHashSet(this.expectedTraits);
        expected.remove(StoreTrait.QUERY_AGGREGATION);
        expected.remove(StoreTrait.VISIBILITY);
        expected.remove(StoreTrait.INGEST_AGGREGATION);
        assertEquals(expected, actual);
    }


    @Test
    public void shouldGetTraitsForSchemaWithVisibility() throws Exception {
        // When
        final Set<StoreTrait> actual = getStoreTraits(new Schema.Builder()
                .visibilityProperty(STRING)
                .build());

        // Then
        final Set<StoreTrait> expected = Sets.newHashSet(this.expectedTraits);
        expected.remove(StoreTrait.QUERY_AGGREGATION);
        expected.remove(StoreTrait.STORE_VALIDATION);
        expected.remove(StoreTrait.INGEST_AGGREGATION);
        assertEquals(expected, actual);
    }

    @Test
    public void shouldGetTraitsForSchemaWithAggregatorAndGroupBy() throws Exception {
        // When
        final Set<StoreTrait> actual = getStoreTraits(new Schema.Builder()
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

        // Then
        final Set<StoreTrait> expected = Sets.newHashSet(this.expectedTraits);
        expected.remove(StoreTrait.STORE_VALIDATION);
        expected.remove(StoreTrait.VISIBILITY);
        assertEquals(expected, actual);
    }

    @Test
    public void shouldHaveAllTraitsForSupported() throws Exception {
        // Given
        store.initialise(STORE_ID, new Schema(), new StoreProperties());

        // When
        Set<StoreTrait> traits = store.execute(
                new GetTraits.Builder()
                        .currentTraits(false)
                        .build(),
                new Context(testUser()));

        // Then
        assertEquals(expectedTraits, traits);
    }

    private Set<StoreTrait> getStoreTraits(final Schema schema) throws StoreException, uk.gov.gchq.gaffer.operation.OperationException {
        store.initialise(STORE_ID, schema, new StoreProperties());
        Set<StoreTrait> execute = store.execute(
                new GetTraits.Builder()
                        .currentTraits(true)
                        .build(),
                new Context(testUser()));

        final Set<StoreTrait> actual = Sets.newHashSet(execute);
        assertFalse(actual.isEmpty());
        return actual;
    }
}
