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

import gaffer.accumulostore.key.AccumuloElementConverter;
import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.key.exception.AggregationException;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.accumulostore.utils.IteratorOptionsBuilder;
import gaffer.data.element.Properties;
import gaffer.data.element.function.ElementAggregator;
import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.data.elementdefinition.schema.exception.SchemaException;
import gaffer.store.schema.StoreSchema;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Map;

/**
 * The aggregator iterator is used to combine {@link Value}s where the
 * {@link Key} is the same (Except for the Timestamp column). The instructions
 * provided in the data schema define how the aggregation takes place and
 * therefore what the resulting {@link Value} will be.
 */
public class AggregatorIterator extends Combiner {
    private DataSchema dataSchema;
    private AccumuloElementConverter elementConverter;

    @Override
    public Value reduce(final Key key, final Iterator<Value> iter) {
        // Get first Value. If this is the only Value then return it straight
        // away;
        Value value = iter.next();
        if (!iter.hasNext()) {
            return value;
        }
        final String group;
        try {
            group = new String(key.getColumnFamilyData().getBackingArray(), AccumuloStoreConstants.UTF_8_CHARSET);
        } catch (final UnsupportedEncodingException e) {
            throw new AggregationException("Failed to recreate a graph element from a key and value", e);
        }

        Properties properties;
        final ElementAggregator aggregator;
        try {
            properties = elementConverter.getPropertiesFromValue(group, value);
        } catch (final AccumuloElementConversionException e) {
            throw new AggregationException("Failed to recreate a graph element from a key and value", e);
        }
        aggregator = dataSchema.getElement(group).getAggregator();
        aggregator.aggregate(properties);
        while (iter.hasNext()) {
            value = iter.next();
            try {
                properties = elementConverter.getPropertiesFromValue(group, value);
            } catch (final AccumuloElementConversionException e) {
                throw new AggregationException("Failed to recreate a graph element from a key and value", e);
            }
            aggregator.aggregate(properties);
        }
        properties = new Properties();
        aggregator.state(properties);
        try {
            return elementConverter.getValueFromProperties(properties, group);
        } catch (final AccumuloElementConversionException e) {
            throw new AggregationException("Failed to create an accumulo value from an elements properties", e);
        }
    }

    @Override
    public void init(final SortedKeyValueIterator<Key, Value> source, final Map<String, String> options,
            final IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        validateOptions(options);
    }

    @Override
    public boolean validateOptions(final Map<String, String> options) {
        if (!super.validateOptions(options)) {
            return false;
        }
        if (!options.containsKey(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS)) {
            throw new IllegalArgumentException("Must specify the " + AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS);
        }
        if (!options.containsKey(AccumuloStoreConstants.STORE_SCHEMA)) {
            throw new IllegalArgumentException("Must specify the " + AccumuloStoreConstants.STORE_SCHEMA);
        }
        if (!options.containsKey(AccumuloStoreConstants.DATA_SCHEMA)) {
            throw new IllegalArgumentException("Must specify the " + AccumuloStoreConstants.DATA_SCHEMA);
        }

        final StoreSchema storeSchema;
        try {
            dataSchema = DataSchema.fromJson(options.get(AccumuloStoreConstants.DATA_SCHEMA).getBytes(AccumuloStoreConstants.UTF_8_CHARSET));
            storeSchema = StoreSchema.fromJson(options.get(AccumuloStoreConstants.STORE_SCHEMA).getBytes(AccumuloStoreConstants.UTF_8_CHARSET));
        } catch (final UnsupportedEncodingException e) {
            throw new SchemaException("Unable to deserialise the data/store schema", e);
        }

        try {
            final Class<?> elementConverterClass = Class
                    .forName(options.get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS));
            elementConverter = (AccumuloElementConverter) elementConverterClass.getConstructor(StoreSchema.class)
                    .newInstance(storeSchema);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new AggregationException("Failed to load element converter from class name provided : "
                    + options.get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS));
        }
        return true;
    }

    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptionsBuilder(super.describeOptions()).addDataSchemaNamedOption()
                .addStoreSchemaNamedOption().addElementConverterClassNamedOption()
                .setIteratorName(AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME)
                .setIteratorDescription(
                        "Applies a reduce function to elements with identical (rowKey, column family, column qualifier, visibility)")
                .build();
    }

}
