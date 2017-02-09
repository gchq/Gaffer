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

package uk.gov.gchq.gaffer.accumulostore.key.impl;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.function.ExampleFilterFunction;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter.Builder;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValidatorFilterTest {
    @Test
    public void shouldThrowIllegalArgumentExceptionWhenValidateOptionsWithNoSchema() throws Exception {
        // Given
        final ValidatorFilter filter = new ValidatorFilter();


        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ByteEntityAccumuloElementConverter.class.getName());

        // When / Then
        try {
            filter.validateOptions(options);
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(AccumuloStoreConstants.SCHEMA));
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenValidateOptionsWithElementConverterClass() throws Exception {
        // Given
        final ValidatorFilter filter = new ValidatorFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.SCHEMA, getSchemaJson());

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
        final ValidatorFilter filter = new ValidatorFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.SCHEMA, getSchemaJson());
        options.put(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ByteEntityAccumuloElementConverter.class.getName());

        // When
        final boolean isValid = filter.validateOptions(options);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldAcceptElementWhenSchemaValidatorAcceptsElement() throws Exception {
        // Given
        final ValidatorFilter filter = new ValidatorFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.SCHEMA, getSchemaJson());
        options.put(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ByteEntityAccumuloElementConverter.class.getName());

        filter.validateOptions(options);

        final ByteEntityAccumuloElementConverter converter = new ByteEntityAccumuloElementConverter(getSchema());

        final Element element = new Edge(TestGroups.EDGE, "source", "dest", true);
        final Pair<Key> key = converter.getKeysFromElement(element);
        final Value value = converter.getValueFromElement(element);

        // When
        final boolean accept = filter.accept(key.getFirst(), value);

        // Then
        assertTrue(accept);
    }

    @Test
    public void shouldNotAcceptElementWhenSchemaValidatorDoesNotAcceptElement() throws Exception {
        // Given
        final ValidatorFilter filter = new ValidatorFilter();

        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.SCHEMA, getSchemaJson());
        options.put(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ByteEntityAccumuloElementConverter.class.getName());

        filter.validateOptions(options);

        final ByteEntityAccumuloElementConverter converter = new ByteEntityAccumuloElementConverter(getSchema());

        final Element element = new Edge(TestGroups.EDGE, "invalid", "dest", true);
        final Pair<Key> key = converter.getKeysFromElement(element);
        final Value value = converter.getValueFromElement(element);

        // When
        final boolean accept = filter.accept(key.getFirst(), value);

        // Then
        assertFalse(accept);
    }

    private String getSchemaJson() throws UnsupportedEncodingException {
        return new String(getSchema().toCompactJson(), CommonConstants.UTF_8);
    }

    private Schema getSchema() throws UnsupportedEncodingException {
        return new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .validator(new Builder()
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .type(TestTypes.DIRECTED_TRUE, new TypeDefinition.Builder()
                        .clazz(Boolean.class)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .build())
                .vertexSerialiser(new StringSerialiser())
                .build();
    }
}