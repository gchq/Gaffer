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
import gaffer.accumulostore.utils.Constants;
import gaffer.accumulostore.utils.Pair;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.schema.DataEdgeDefinition;
import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.store.schema.StoreElementDefinition;
import gaffer.store.schema.StoreSchema;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class ElementValidatorFilterTest {
    @Test
    public void shouldThrowIllegalArgumentExceptionWhenValidateOptionsWithNoStoreSchema() throws Exception {
        // Given
        final ElementValidatorFilter filter = new ElementValidatorFilter();


        final Map<String, String> options = new HashMap<>();
        options.put(Constants.DATA_SCHEMA, getDataSchemaJson());
        options.put(Constants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ByteEntityAccumuloElementConverter.class.getName());

        // When / Then
        try {
            filter.validateOptions(options);
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(Constants.STORE_SCHEMA));
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenValidateOptionsWithNoDataSchema() throws Exception {
        // Given
        final ElementValidatorFilter filter = new ElementValidatorFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(Constants.STORE_SCHEMA, getStoreSchemaJson());
        options.put(Constants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ByteEntityAccumuloElementConverter.class.getName());

        // When / Then
        try {
            filter.validateOptions(options);
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(Constants.DATA_SCHEMA));
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenValidateOptionsWithElementConverterClass() throws Exception {
        // Given
        final ElementValidatorFilter filter = new ElementValidatorFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(Constants.STORE_SCHEMA, getStoreSchemaJson());
        options.put(Constants.DATA_SCHEMA, getDataSchemaJson());

        // When / Then
        try {
            filter.validateOptions(options);
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(Constants.ACCUMULO_ELEMENT_CONVERTER_CLASS));
        }
    }

    @Test
    public void shouldReturnTrueWhenValidOptions() throws Exception {
        // Given
        final ElementValidatorFilter filter = new ElementValidatorFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(Constants.STORE_SCHEMA, getStoreSchemaJson());
        options.put(Constants.DATA_SCHEMA, getDataSchemaJson());
        options.put(Constants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ByteEntityAccumuloElementConverter.class.getName());

        // When
        final boolean isValid = filter.validateOptions(options);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldAcceptElementWhenDataSchemaValidatorAcceptsElement() throws Exception {
        // Given
        final ElementValidatorFilter filter = new ElementValidatorFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(Constants.STORE_SCHEMA, getStoreSchemaJson());
        options.put(Constants.DATA_SCHEMA, getDataSchemaJson());
        options.put(Constants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ByteEntityAccumuloElementConverter.class.getName());

        filter.validateOptions(options);

        final ByteEntityAccumuloElementConverter converter = new ByteEntityAccumuloElementConverter(getStoreSchema());

        final Element element = new Edge("BasicEdge", "source", "dest", true);
        final Pair<Key> key = converter.getKeysFromElement(element);
        final Value value = converter.getValueFromElement(element);

        // When
        final boolean accept = filter.accept(key.getFirst(), value);

        // Then
        assertTrue(accept);
    }

    @Test
    public void shouldNotAcceptElementWhenDataSchemaValidatorDoesNotAcceptElement() throws Exception {
        // Given
        final ElementValidatorFilter filter = new ElementValidatorFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(Constants.STORE_SCHEMA, getStoreSchemaJson());
        options.put(Constants.DATA_SCHEMA, getEmptyDataSchemaJson());
        options.put(Constants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ByteEntityAccumuloElementConverter.class.getName());

        filter.validateOptions(options);

        final ByteEntityAccumuloElementConverter converter = new ByteEntityAccumuloElementConverter(getStoreSchema());

        final Element element = new Edge("BasicEdge", "source", "dest", true);
        final Pair<Key> key = converter.getKeysFromElement(element);
        final Value value = converter.getValueFromElement(element);

        // When
        final boolean accept = filter.accept(key.getFirst(), value);

        // Then
        assertFalse(accept);
    }

    private String getDataSchemaJson() throws UnsupportedEncodingException {
        final DataSchema dataSchema = new DataSchema.Builder()
                .edge("BasicEdge", new DataEdgeDefinition.Builder()
                        .build())
                .build();

        return new String(dataSchema.toJson(false), Constants.UTF_8_CHARSET);
    }

    private String getEmptyDataSchemaJson() throws UnsupportedEncodingException {
        final DataSchema dataSchema = new DataSchema.Builder()
                .build();

        return new String(dataSchema.toJson(false), Constants.UTF_8_CHARSET);
    }

    private StoreSchema getStoreSchema() throws UnsupportedEncodingException {
        return new StoreSchema.Builder()
                .edge("BasicEdge", new StoreElementDefinition.Builder()
                        .build())
                .build();
    }

    private String getStoreSchemaJson() throws UnsupportedEncodingException {
        return new String(getStoreSchema().toJson(false), Constants.UTF_8_CHARSET);
    }
}