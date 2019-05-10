/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.key.impl;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.key.AbstractElementFilter;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ElementPostAggregationFilterTest {
    @Test
    public void shouldThrowIllegalArgumentExceptionWhenValidateOptionsWithNoSchema() throws Exception {
        // Given
        final AbstractElementFilter filter = new ElementPostAggregationFilter();


        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.VIEW, getViewJson());
        options.put(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ByteEntityAccumuloElementConverter.class.getName());

        // When / Then
        try {
            filter.validateOptions(options);
            fail("Expected IllegalArgumentException to be thrown on method invocation");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(AccumuloStoreConstants.SCHEMA));
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenInitWithNoView() throws Exception {
        // Given
        final AbstractElementFilter filter = new ElementPostAggregationFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.SCHEMA, getSchemaJson());
        options.put(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ByteEntityAccumuloElementConverter.class.getName());

        // When / Then
        try {
            filter.init(null, options, null);
            fail("Expected IllegalArgumentException to be thrown on method invocation");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(AccumuloStoreConstants.VIEW));
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenValidateOptionsWithElementConverterClass() throws Exception {
        // Given
        final AbstractElementFilter filter = new ElementPostAggregationFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.SCHEMA, getSchemaJson());
        options.put(AccumuloStoreConstants.VIEW, getViewJson());

        // When / Then
        try {
            filter.validateOptions(options);
            fail("Expected IllegalArgumentException to be thrown on method invocation");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS));
        }
    }

    @Test
    public void shouldReturnTrueWhenValidOptions() throws Exception {
        // Given
        final AbstractElementFilter filter = new ElementPostAggregationFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.SCHEMA, getSchemaJson());
        options.put(AccumuloStoreConstants.VIEW, getViewJson());
        options.put(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ByteEntityAccumuloElementConverter.class.getName());

        // When
        final boolean isValid = filter.validateOptions(options);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldAcceptElementWhenViewValidatorAcceptsElement() throws Exception {
        // Given
        final AbstractElementFilter filter = new ElementPostAggregationFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.SCHEMA, getSchemaJson());
        options.put(AccumuloStoreConstants.VIEW, getViewJson());
        options.put(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ByteEntityAccumuloElementConverter.class.getName());

        filter.init(null, options, null);

        final ByteEntityAccumuloElementConverter converter = new ByteEntityAccumuloElementConverter(getSchema());

        final Element element = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("source")
                .dest("dest")
                .directed(true)
                .build();
        final Pair<Key, Key> key = converter.getKeysFromElement(element);
        final Value value = converter.getValueFromElement(element);

        // When
        final boolean accept = filter.accept(key.getFirst(), value);

        // Then
        assertTrue(accept);
    }

    @Test
    public void shouldNotAcceptElementWhenViewValidatorDoesNotAcceptElement() throws Exception {
        // Given
        final AbstractElementFilter filter = new ElementPostAggregationFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.SCHEMA, getSchemaJson());
        options.put(AccumuloStoreConstants.VIEW, getEmptyViewJson());
        options.put(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ByteEntityAccumuloElementConverter.class.getName());

        filter.init(null, options, null);

        final ByteEntityAccumuloElementConverter converter = new ByteEntityAccumuloElementConverter(getSchema());

        final Element element = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("source")
                .dest("dest")
                .directed(true)
                .build();
        final Pair<Key, Key> key = converter.getKeysFromElement(element);
        final Value value = converter.getValueFromElement(element);

        // When
        final boolean accept = filter.accept(key.getFirst(), value);

        // Then
        assertFalse(accept);
    }

    private String getViewJson() throws UnsupportedEncodingException {
        final View view = new View.Builder()
                .edge(TestGroups.EDGE)
                .build();

        return new String(view.toCompactJson(), CommonConstants.UTF_8);
    }

    private String getEmptyViewJson() throws UnsupportedEncodingException {
        final View view = new View.Builder()
                .build();

        return new String(view.toCompactJson(), CommonConstants.UTF_8);
    }

    private Schema getSchema() throws UnsupportedEncodingException {
        return new Schema.Builder()
                .type("string", String.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .build())
                .vertexSerialiser(new StringSerialiser())
                .build();
    }

    private String getSchemaJson() throws UnsupportedEncodingException {
        return new String(getSchema().toCompactJson(), CommonConstants.UTF_8);
    }
}
