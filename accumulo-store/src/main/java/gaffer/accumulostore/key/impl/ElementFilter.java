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

import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.utils.Constants;
import gaffer.accumulostore.key.AccumuloElementConverter;
import gaffer.accumulostore.key.exception.ElementFilterException;
import gaffer.data.ElementValidator;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.schema.exception.SchemaException;
import gaffer.data.elementdefinition.view.View;
import gaffer.store.schema.StoreSchema;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * The ElementFilter will filter out {@link Element}s based on the filtering instructions given in the {@link View} that is passed to this iterator
 */
public class ElementFilter extends Filter {
    private ElementValidator validator;
    private AccumuloElementConverter elementConverter;

    @Override
    public boolean accept(final Key key, final Value value) {
        final Element element;
        try {
            element = elementConverter.getFullElement(key, value);
        } catch (AccumuloElementConversionException e) {
            throw new ElementFilterException("Element filter iterator failed to crete an element from an accumulo gaffer.accumulostore.key value pair", e);
        }
        return validator.validate(element);
    }

    @Override
    public void init(final SortedKeyValueIterator<Key, Value> source, final Map<String, String> options, final IteratorEnvironment env) throws IOException {
        validateOptions(options);
        super.init(source, options, env);
    }

    @Override
    public boolean validateOptions(final Map<String, String> options) {
        if (!options.containsKey(Constants.STORE_SCHEMA)) {
            throw new IllegalArgumentException("Must specify the " + Constants.STORE_SCHEMA);
        }
        if (!options.containsKey(Constants.VIEW)) {
            throw new IllegalArgumentException("Must specify the " + Constants.VIEW);
        }
        try {
            validator = new ElementValidator(View.fromJson(options.get(Constants.VIEW).getBytes(Constants.UTF_8_CHARSET)));
        } catch (UnsupportedEncodingException e) {
            throw new SchemaException("Unable to deserialise view from JSON", e);

        }

        final StoreSchema storeSchema;
        try {
            storeSchema = StoreSchema.fromJson(options.get(Constants.STORE_SCHEMA).getBytes(Constants.UTF_8_CHARSET));
        } catch (UnsupportedEncodingException e) {
            throw new ElementFilterException(e.getMessage(), e);
        }

        try {
            Class<?> elementConverterClass = Class.forName(options.get(Constants.ACCUMULO_ELEMENT_CONVERTER_CLASS));
            elementConverter = (AccumuloElementConverter) elementConverterClass.getConstructor(StoreSchema.class).newInstance(storeSchema);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException e) {
            throw new ElementFilterException("Failed to load element converter from class name provided : " + options.get(Constants.ACCUMULO_ELEMENT_CONVERTER_CLASS));
        }
        return true;
    }

    @Override
    public IteratorOptions describeOptions() {
        final Map<String, String> namedOptions = new HashMap<>();
        namedOptions.put(Constants.VIEW, "A serialised gaffer.accumulostore.operation schema");
        namedOptions.put(Constants.STORE_SCHEMA, "A serialised gaffer.accumulostore schema");
        return new IteratorOptions(Constants.VIEW,
                "Only returns elements that match the supplied predicates",
                namedOptions, null);
    }
}
