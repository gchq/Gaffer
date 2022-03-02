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

package uk.gov.gchq.gaffer.store.operation.handler;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.HasTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.predicate.Exists;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class HasTraitHandlerTest {

    public static final String STORE_ID = "StoreId";
    public static final String STRING = "string";
    private static final StoreTrait VISIBILITY = StoreTrait.VISIBILITY;
    private static final StoreTrait QUERY_AGGREGATION = StoreTrait.QUERY_AGGREGATION;
    private static final StoreTrait TRANSFORMATION = StoreTrait.TRANSFORMATION;
    private Store store;
    private Set<StoreTrait> expectedTraits;
    private Schema string;

    @BeforeEach
    public void setUp() throws Exception {
        expectedTraits = Sets.newHashSet(StoreTrait.ALL_TRAITS);
        expectedTraits.remove(StoreTrait.ORDERED);

        store = new TestAddToGraphLibraryImpl() {
            @Override
            public Set<StoreTrait> getTraits() {
                return Sets.newHashSet(expectedTraits);
            }
            @Override
            protected OutputOperationHandler<GetTraits, Set<StoreTrait>> getGetTraitsHandler() {
                return new GetTraitsHandler(expectedTraits);
            }
        };
        assertNotEquals(StoreTrait.ALL_TRAITS, expectedTraits);
        string = new Schema.Builder().type(STRING, String.class).build();
    }

    @AfterEach
    public void tearDown() throws Exception {
        final Set<StoreTrait> temp = Sets.newHashSet(StoreTrait.ALL_TRAITS);
        temp.remove(StoreTrait.ORDERED);
        assertEquals(temp, this.expectedTraits);
        assertNotEquals(StoreTrait.ALL_TRAITS, expectedTraits);
    }
    @Test
    public void shouldHighlightDifferenceWhenSettingCurrentTraitsOption() throws Exception {
        // When
        Boolean presentInDefaultTraits = hasStoreTrait(new Schema(), QUERY_AGGREGATION, false);
        Boolean presentInCurrentTraits = hasStoreTrait(new Schema(), QUERY_AGGREGATION, true);

        // Then
        assertFalse(presentInCurrentTraits);
        assertTrue(presentInDefaultTraits);

    }

    @Test
    public void shouldCheckForTraitForSchemaEmpty() throws Exception {
        // When
        final Boolean present = hasStoreTrait(new Schema(), TRANSFORMATION, true);
        final Boolean absent = hasStoreTrait(new Schema(),  QUERY_AGGREGATION, true);

        // Then
        assertTrue(present);
        assertFalse(absent);
    }

    @Test
    public void shouldCheckForTraitForSchemaWithGroupBy() throws Exception {
        // When
        final Boolean present = hasStoreTrait(new Schema.Builder()
                .entity("e1", new SchemaEntityDefinition.Builder()
                        .groupBy("gb")
                        .vertex(STRING)
                        .build())
                .merge(string)
                .build(), TRANSFORMATION, true);

        final Boolean absent = hasStoreTrait(new Schema.Builder()
                .entity("e1", new SchemaEntityDefinition.Builder()
                        .groupBy("gb")
                        .vertex(STRING)
                        .build())
                .merge(string)
                .build(), VISIBILITY, true);

        //Then
        assertTrue(present);
        assertFalse(absent);
    }

    @Test
    public void shouldCheckForTraitForSchemaWithValidator() throws Exception {
        // When
        final Boolean present = hasStoreTrait(new Schema.Builder()
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
                .build(), TRANSFORMATION, true);

        final Boolean absent = hasStoreTrait(new Schema.Builder()
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
                .build(),  QUERY_AGGREGATION, true);

        //Then
        assertTrue(present);
        assertFalse(absent);
    }


    @Test
    public void shouldCheckForTraitForSchemaWithVisibility() throws Exception {
        // When
        final Boolean present = hasStoreTrait(new Schema.Builder()
                .visibilityProperty(STRING)
                .build(), TRANSFORMATION, true);

        final Boolean absent = hasStoreTrait(new Schema.Builder()
                .visibilityProperty(STRING)
                .build(),  QUERY_AGGREGATION, true);

        //Then
        assertTrue(present);
        assertFalse(absent);
    }

    @Test
    public void shouldGetTraitsForSchemaWithAggregatorAndGroupBy() throws Exception {
        // When
        final Boolean present = hasStoreTrait(new Schema.Builder()
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
                .build(), TRANSFORMATION, true);

        final Boolean absent = hasStoreTrait(new Schema.Builder()
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
                        .build(), VISIBILITY, true);

        //Then
        assertTrue(present);
        assertFalse(absent);
    }

    private Boolean hasStoreTrait(final Schema schema, final StoreTrait trait, Boolean currentTraitsFlag) throws StoreException, uk.gov.gchq.gaffer.operation.OperationException {
        store.initialise(STORE_ID, schema, new StoreProperties());
        return store.execute(
                new HasTrait.Builder()
                        .currentTraits(currentTraitsFlag)
                        .trait(trait)
                        .build(),
                new Context(testUser()));
    }
}
