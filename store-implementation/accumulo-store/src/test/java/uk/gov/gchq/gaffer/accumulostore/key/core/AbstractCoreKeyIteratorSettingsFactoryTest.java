/*
 * Copyright 2016-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.accumulostore.key.core;

import org.apache.accumulo.core.client.IteratorSetting;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloKeyPackage;
import uk.gov.gchq.gaffer.accumulostore.key.impl.ElementPostAggregationFilter;
import uk.gov.gchq.gaffer.accumulostore.key.impl.ElementPreAggregationFilter;
import uk.gov.gchq.gaffer.accumulostore.key.impl.ValidatorFilter;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.predicate.Exists;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public abstract class AbstractCoreKeyIteratorSettingsFactoryTest {
    private final AbstractCoreKeyIteratorSettingsFactory factory;

    protected AbstractCoreKeyIteratorSettingsFactoryTest(final AbstractCoreKeyIteratorSettingsFactory factory) {
        this.factory = factory;
    }

    @Test
    public void shouldReturnNullValidatorIteratorIfNoSchemaValidation() throws Exception {
        // Given
        final AccumuloStore store = mock(AccumuloStore.class);
        final Schema schema = createSchema();
        given(store.getSchema()).willReturn(schema);

        // When
        final IteratorSetting iterator = factory.getValidatorIteratorSetting(store);

        // Then
        assertNull(iterator);
    }

    @Test
    public void shouldReturnIteratorValidatorIterator() throws Exception {
        // Given
        final AccumuloStore store = mock(AccumuloStore.class);
        final Schema schema = new Schema.Builder()
                .merge(createSchema())
                .type("str", new TypeDefinition.Builder()
                        .validateFunctions(new Exists())
                        .build())
                .build();
        final AccumuloKeyPackage keyPackage = mock(AccumuloKeyPackage.class);
        final AccumuloElementConverter converter = mock(AccumuloElementConverter.class);

        given(store.getSchema()).willReturn(schema);
        given(store.getKeyPackage()).willReturn(keyPackage);
        given(keyPackage.getKeyConverter()).willReturn(converter);

        // When
        final IteratorSetting iterator = factory.getValidatorIteratorSetting(store);

        // Then
        assertEquals(AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME, iterator.getName());
        assertEquals(AccumuloStoreConstants.VALIDATOR_ITERATOR_PRIORITY, iterator.getPriority());
        assertEquals(ValidatorFilter.class.getName(), iterator.getIteratorClass());
        JsonAssert.assertEquals(schema.toCompactJson(), iterator.getOptions().get(AccumuloStoreConstants.SCHEMA).getBytes());
        assertEquals(converter.getClass().getName(), iterator.getOptions().get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS));
    }

    @Test
    public void shouldReturnNullPreAggFilterIfNoPreAggFilters() throws Exception {
        // Given
        final AccumuloStore store = mock(AccumuloStore.class);
        final Schema schema = createSchema();
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .postAggregationFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new Exists())
                                .build())
                        .build())
                .build();
        given(store.getSchema()).willReturn(schema);

        // When
        final IteratorSetting iterator = factory.getElementPreAggregationFilterIteratorSetting(view, store);

        // Then
        assertNull(iterator);
    }

    @Test
    public void shouldReturnPreAggFilterIterator() throws Exception {
        // Given
        final AccumuloStore store = mock(AccumuloStore.class);
        final Schema schema = createSchema();
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new Exists())
                                .build())
                        .build())
                .build();
        final AccumuloKeyPackage keyPackage = mock(AccumuloKeyPackage.class);
        final AccumuloElementConverter converter = mock(AccumuloElementConverter.class);

        given(store.getSchema()).willReturn(schema);
        given(store.getKeyPackage()).willReturn(keyPackage);
        given(keyPackage.getKeyConverter()).willReturn(converter);

        // When
        final IteratorSetting iterator = factory.getElementPreAggregationFilterIteratorSetting(view, store);

        // Then
        assertEquals(AccumuloStoreConstants.ELEMENT_PRE_AGGREGATION_FILTER_ITERATOR_NAME, iterator.getName());
        assertEquals(AccumuloStoreConstants.ELEMENT_PRE_AGGREGATION_FILTER_ITERATOR_PRIORITY, iterator.getPriority());
        assertEquals(ElementPreAggregationFilter.class.getName(), iterator.getIteratorClass());
        JsonAssert.assertEquals(schema.toCompactJson(), iterator.getOptions().get(AccumuloStoreConstants.SCHEMA).getBytes());
        JsonAssert.assertEquals(view.toCompactJson(), iterator.getOptions().get(AccumuloStoreConstants.VIEW).getBytes());
        assertEquals(converter.getClass().getName(), iterator.getOptions().get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS));
    }

    @Test
    public void shouldReturnNullPostAggFilterIfNoPreAggFilters() throws Exception {
        // Given
        final AccumuloStore store = mock(AccumuloStore.class);
        final Schema schema = createSchema();
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new Exists())
                                .build())
                        .build())
                .build();
        given(store.getSchema()).willReturn(schema);

        // When
        final IteratorSetting iterator = factory.getElementPostAggregationFilterIteratorSetting(view, store);

        // Then
        assertNull(iterator);
    }

    @Test
    public void shouldReturnPostAggFilterIterator() throws Exception {
        // Given
        final AccumuloStore store = mock(AccumuloStore.class);
        final Schema schema = createSchema();
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .postAggregationFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new Exists())
                                .build())
                        .build())
                .build();
        final AccumuloKeyPackage keyPackage = mock(AccumuloKeyPackage.class);
        final AccumuloElementConverter converter = mock(AccumuloElementConverter.class);

        given(store.getSchema()).willReturn(schema);
        given(store.getKeyPackage()).willReturn(keyPackage);
        given(keyPackage.getKeyConverter()).willReturn(converter);

        // When
        final IteratorSetting iterator = factory.getElementPostAggregationFilterIteratorSetting(view, store);

        // Then
        assertEquals(AccumuloStoreConstants.ELEMENT_POST_AGGREGATION_FILTER_ITERATOR_NAME, iterator.getName());
        assertEquals(AccumuloStoreConstants.ELEMENT_POST_AGGREGATION_FILTER_ITERATOR_PRIORITY, iterator.getPriority());
        assertEquals(ElementPostAggregationFilter.class.getName(), iterator.getIteratorClass());
        JsonAssert.assertEquals(schema.toCompactJson(), iterator.getOptions().get(AccumuloStoreConstants.SCHEMA).getBytes());
        JsonAssert.assertEquals(view.toCompactJson(), iterator.getOptions().get(AccumuloStoreConstants.VIEW).getBytes());
        assertEquals(converter.getClass().getName(), iterator.getOptions().get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS));
    }

    private Schema createSchema() {
        return new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "str")
                        .build())
                .type("str", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new StringConcat())
                        .build())
                .build();
    }
}
