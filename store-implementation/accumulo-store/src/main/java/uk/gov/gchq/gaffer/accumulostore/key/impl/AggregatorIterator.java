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

package uk.gov.gchq.gaffer.accumulostore.key.impl;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AggregationException;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.IteratorOptionsBuilder;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Map;

/**
 * The aggregator iterator is used to combine {@link Value}s where the
 * {@link Key} is the same (Except for the Timestamp column). The instructions
 * provided in the schema define how the aggregation takes place and
 * therefore what the resulting {@link Value} will be.
 */
public class AggregatorIterator extends Combiner {
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregatorIterator.class);

    private Schema schema;
    private AccumuloElementConverter elementConverter;

    @Override
    public Value reduce(final Key key, final Iterator<Value> iter) {
        // Get first Value. If this is the only Value then return it straight
        // away;
        Value value = iter.next();
        if (!iter.hasNext()) {
            return value;
        }
        final String group = elementConverter.getGroupFromColumnFamily(key.getColumnFamilyData().getBackingArray());
        Properties properties;
        final ElementAggregator aggregator = schema.getElement(group).getIngestAggregator();
        try {
            properties = elementConverter.getPropertiesFromValue(group, value);
        } catch (final AccumuloElementConversionException e) {
            throw new AggregationException("Failed to recreate a graph element from a key and value", e);
        }
        Properties aggregatedProps = properties;
        while (iter.hasNext()) {
            value = iter.next();
            try {
                properties = elementConverter.getPropertiesFromValue(group, value);
            } catch (final AccumuloElementConversionException e) {
                throw new AggregationException("Failed to recreate a graph element from a key and value", e);
            }
            aggregatedProps = aggregator.apply(aggregatedProps, properties);
        }
        try {
            return elementConverter.getValueFromProperties(group, aggregatedProps);
        } catch (final AccumuloElementConversionException e) {
            throw new AggregationException("Failed to create an accumulo value from an elements properties", e);
        }
    }

    @Override
    public void init(final SortedKeyValueIterator<Key, Value> source, final Map<String, String> options,
                     final IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        try {
            schema = Schema.fromJson(options.get(AccumuloStoreConstants.SCHEMA).getBytes(CommonConstants.UTF_8));
        } catch (final UnsupportedEncodingException e) {
            throw new SchemaException("Unable to deserialise the schema from json", e);
        }
        LOGGER.debug("Initialising AggregatorIterator with schema {}", schema);

        final String elementConverterClass = options.get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS);
        try {
            elementConverter = Class
                    .forName(elementConverterClass)
                    .asSubclass(AccumuloElementConverter.class)
                    .getConstructor(Schema.class)
                    .newInstance(schema);
            LOGGER.debug("Creating AccumuloElementConverter of class {}", elementConverterClass);
        } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new AggregationException("Failed to create element converter of the class name provided ("
                    + elementConverterClass + ")", e);
        }
    }

    @Override
    public boolean validateOptions(final Map<String, String> options) {
        if (!super.validateOptions(options)) {
            return false;
        }
        if (!options.containsKey(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS)) {
            throw new IllegalArgumentException("Must specify the " + AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS);
        }
        if (!options.containsKey(AccumuloStoreConstants.SCHEMA)) {
            throw new IllegalArgumentException("Must specify the " + AccumuloStoreConstants.SCHEMA);
        }
        return true;
    }

    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptionsBuilder(super.describeOptions()).addSchemaNamedOption()
                .addSchemaNamedOption().addElementConverterClassNamedOption()
                .setIteratorName(AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME)
                .setIteratorDescription(
                        "Applies a reduce function to elements with identical (rowKey, column family, column qualifier, visibility)")
                .build();
    }

}
