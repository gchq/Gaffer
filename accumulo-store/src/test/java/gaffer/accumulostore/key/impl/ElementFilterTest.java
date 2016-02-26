/*
 * Copyright 2016 Crown Copyright
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

package gaffer.accumulostore.key.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.accumulostore.utils.Pair;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.store.schema.DataSchema;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class ElementFilterTest {
    @Test
    public void shouldThrowIllegalArgumentExceptionWhenValidateOptionsWithNoDataSchema() throws Exception {
        // Given
        final ElementFilter filter = new ElementFilter();


        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.VIEW, getViewJson());
        options.put(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ByteEntityAccumuloElementConverter.class.getName());

        // When / Then
        try {
            filter.validateOptions(options);
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(AccumuloStoreConstants.DATA_SCHEMA));
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenValidateOptionsWithNoView() throws Exception {
        // Given
        final ElementFilter filter = new ElementFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.DATA_SCHEMA, getDataSchemaJson());
        options.put(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ByteEntityAccumuloElementConverter.class.getName());

        // When / Then
        try {
            filter.validateOptions(options);
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(AccumuloStoreConstants.VIEW));
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenValidateOptionsWithElementConverterClass() throws Exception {
        // Given
        final ElementFilter filter = new ElementFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.DATA_SCHEMA, getDataSchemaJson());
        options.put(AccumuloStoreConstants.VIEW, getViewJson());

        // When / Then
        try {
            filter.validateOptions(options);
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS));
        }
    }

    @Test
    public void shouldReturnTrueWhenValidOptions() throws Exception {
        // Given
        final ElementFilter filter = new ElementFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.DATA_SCHEMA, getDataSchemaJson());
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
        final ElementFilter filter = new ElementFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.DATA_SCHEMA, getDataSchemaJson());
        options.put(AccumuloStoreConstants.VIEW, getViewJson());
        options.put(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ByteEntityAccumuloElementConverter.class.getName());

        filter.validateOptions(options);

        final ByteEntityAccumuloElementConverter converter = new ByteEntityAccumuloElementConverter(getDataSchema());

        final Element element = new Edge("BasicEdge", "source", "dest", true);
        final Pair<Key> key = converter.getKeysFromElement(element);
        final Value value = converter.getValueFromElement(element);

        // When
        final boolean accept = filter.accept(key.getFirst(), value);

        // Then
        assertTrue(accept);
    }

    @Test
    public void shouldNotAcceptElementWhenViewValidatorDoesNotAcceptElement() throws Exception {
        // Given
        final ElementFilter filter = new ElementFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.DATA_SCHEMA, getDataSchemaJson());
        options.put(AccumuloStoreConstants.VIEW, getEmptyViewJson());
        options.put(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ByteEntityAccumuloElementConverter.class.getName());

        filter.validateOptions(options);

        final ByteEntityAccumuloElementConverter converter = new ByteEntityAccumuloElementConverter(getDataSchema());

        final Element element = new Edge("BasicEdge", "source", "dest", true);
        final Pair<Key> key = converter.getKeysFromElement(element);
        final Value value = converter.getValueFromElement(element);

        // When
        final boolean accept = filter.accept(key.getFirst(), value);

        // Then
        assertFalse(accept);
    }

    private String getViewJson() throws UnsupportedEncodingException {
        final View view = new View.Builder()
                .edge("BasicEdge")
                .build();

        return new String(view.toJson(false), AccumuloStoreConstants.UTF_8_CHARSET);
    }

    private String getEmptyViewJson() throws UnsupportedEncodingException {
        final View view = new View.Builder()
                .build();

        return new String(view.toJson(false), AccumuloStoreConstants.UTF_8_CHARSET);
    }

    private DataSchema getDataSchema() throws UnsupportedEncodingException {
        return new DataSchema.Builder()
                .edge("BasicEdge")
                .build();
    }

    private String getDataSchemaJson() throws UnsupportedEncodingException {
        return new String(getDataSchema().toJson(false), AccumuloStoreConstants.UTF_8_CHARSET);
    }
}