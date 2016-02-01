/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gaffer.accumulostore.key.impl;

import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.key.exception.AggregationException;
import gaffer.accumulostore.utils.Constants;
import gaffer.accumulostore.utils.IteratorUtils;
import gaffer.accumulostore.key.AccumuloElementConverter;
import gaffer.data.element.Properties;
import gaffer.data.element.function.ElementAggregator;
import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.store.schema.StoreSchema;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Map;

/**
 * The aggregator iterator is used to combine {@link Value}s where the {@link Key} is the same.
 * The instructions provided in the data schema define how the aggregation takes place and therefore what the resulting {@link Value}
 * will be.
 */
public class AggregatorIterator extends Combiner {
    private DataSchema dataSchema;
    private AccumuloElementConverter elementConverter;

    @Override
    public Value reduce(final Key key, final Iterator<Value> iter) {
        // Get first Value. If this is the only Value then return it straight away;
        Value value = iter.next();
        if (!iter.hasNext()) {
            return value;
        }
        final String group = new String(key.getColumnFamilyData().getBackingArray());
        Properties properties;
        final ElementAggregator aggregator;
        try {
            properties = elementConverter.getPropertiesFromValue(group, value);
        } catch (AccumuloElementConversionException e) {
            throw new AggregationException("Failed to recreate a graph element from a gaffer.accumulostore.key and value", e);
        }
        aggregator = dataSchema.getElement(group).getAggregator();
        aggregator.aggregate(properties);
        while (iter.hasNext()) {
            value = iter.next();
            try {
                properties = elementConverter.getPropertiesFromValue(group, value);
            } catch (AccumuloElementConversionException e) {
                throw new AggregationException("Failed to recreate a graph element from a gaffer.accumulostore.key and value", e);
            }
            aggregator.aggregate(properties);
        }
        properties = new Properties();
        aggregator.state(properties);
        try {
            return elementConverter.getValueFromProperties(properties, group);
        } catch (AccumuloElementConversionException e) {
            throw new AggregationException("Failed to create an accumulo value from an elements properties", e);
        }
    }

    @Override
    public void init(final SortedKeyValueIterator<Key, Value> source, final Map<String, String> options, final IteratorEnvironment env) throws IOException {
        validateOptions(options);
        super.init(source, options, env);
    }

    @Override
    public boolean validateOptions(final Map<String, String> options) {
        if (!super.validateOptions(options)) {
            return false;
        }
        if (!options.containsKey(Constants.STORE_SCHEMA)) {
            throw new IllegalArgumentException("Must specify the " + Constants.STORE_SCHEMA);
        }
        if (!options.containsKey(Constants.DATA_SCHEMA)) {
            throw new IllegalArgumentException("Must specify the " + Constants.DATA_SCHEMA);
        }
        dataSchema = DataSchema.fromJson(options.get(Constants.DATA_SCHEMA).getBytes());

        final StoreSchema storeSchema = StoreSchema.fromJson(options.get(Constants.STORE_SCHEMA).getBytes());
        try {
            Class<?> elementConverterClass = Class.forName(options.get(Constants.ACCUMULO_KEY_CONVERTER));
            elementConverter = (AccumuloElementConverter) elementConverterClass.getConstructor(StoreSchema.class).newInstance(storeSchema);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException e) {
            throw new AggregationException("Failed to load element converter from class name provided : " + options.get(Constants.ACCUMULO_KEY_CONVERTER));
        }
        return true;
    }

    @Override
    public IteratorOptions describeOptions() {
        return IteratorUtils.describeOptions("AggregatorIterator",
                "Combines properties over elements which have the same key",
                super.describeOptions());
    }
}