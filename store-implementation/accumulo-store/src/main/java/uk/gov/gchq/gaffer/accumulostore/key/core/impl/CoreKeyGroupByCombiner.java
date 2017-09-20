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
package uk.gov.gchq.gaffer.accumulostore.key.core.impl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AggregationException;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.ByteUtils;
import uk.gov.gchq.gaffer.accumulostore.utils.BytesAndRange;
import uk.gov.gchq.gaffer.accumulostore.utils.IteratorOptionsBuilder;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A copy of Accumulo {@link org.apache.accumulo.core.iterators.Combiner} but
 * combining values with identical rowKey and column family.
 * <p>
 * Users extending this class must specify a reduce() method.
 */
public abstract class CoreKeyGroupByCombiner extends WrappingIterator
        implements OptionDescriber {
    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "schema is initialised in validateOptions method, which is always called first")
    protected Schema schema;

    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "view is initialised in init method, which is always called first")
    protected View view;

    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "elementConverter is initialised in init method, which is always called first")
    protected AccumuloElementConverter elementConverter;

    private Key topKey;
    private Value topValue;

    /**
     * A Java Iterator that iterates over the properties for a given row Key
     * and column family from a source {@link SortedKeyValueIterator}.
     */
    public static class KeyValueIterator implements Iterator<Properties> {
        private final Key topKey;
        private final String group;
        private final SortedKeyValueIterator<Key, Value> source;
        private final AccumuloElementConverter elementConverter;
        private final Set<String> groupBy;
        private final Set<String> schemaGroupBy;
        private boolean hasNext;

        /**
         * Constructs an iterator over {@link Value}s whose {@link Key}s are
         * versions of the current topKey of the source
         * {@link SortedKeyValueIterator}.
         *
         * @param source           The {@link SortedKeyValueIterator} of {@link Key},
         *                         {@link Value} pairs from which to read data.
         * @param group            the element group
         * @param elementConverter the elementConverter to use
         * @param schema           the schema
         * @param groupBy          the groupBy properties
         */
        public KeyValueIterator(final SortedKeyValueIterator<Key, Value> source,
                                final String group, final AccumuloElementConverter elementConverter,
                                final Schema schema,
                                final Set<String> groupBy) {
            this.source = source;
            this.group = group;
            this.elementConverter = elementConverter;

            final Key unsafeRef = source.getTopKey();
            topKey = new Key(unsafeRef.getRow().getBytes(),
                    unsafeRef.getColumnFamily().getBytes(),
                    unsafeRef.getColumnQualifier().getBytes(),
                    unsafeRef.getColumnVisibility().getBytes(),
                    unsafeRef.getTimestamp(),
                    unsafeRef.isDeleted(), true);

            schemaGroupBy = schema.getElement(this.group).getGroupBy();
            this.groupBy = groupBy;
            hasNext = _hasNext();
        }

        private boolean _hasNext() {
            return source.hasTop() && !source.getTopKey().isDeleted()
                    && topKey.equals(source.getTopKey(), PartialKey.ROW_COLFAM)
                    && areGroupByPropertiesEqual(topKey, source.getTopKey());
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public Properties next() {
            if (!hasNext) {
                throw new NoSuchElementException();
            }

            final byte[] topColumnQualifier = source.getTopKey().getColumnQualifierData().getBackingArray();
            final byte[] topColumnVisibility = source.getTopKey().getColumnVisibilityData().getBackingArray();
            final long topTimestamp = source.getTopKey().getTimestamp();
            final Value topValue = new Value(source.getTopValue());

            try {
                source.next();
                hasNext = _hasNext();
            } catch (final IOException e) {
                throw new RuntimeException(e); // Looks like a bad idea, but
                // this is what the in-built Combiner iterator does
            }

            final Properties properties = new Properties();
            try {
                properties.putAll(elementConverter.getPropertiesFromColumnQualifier(group, topColumnQualifier));
                properties.putAll(elementConverter.getPropertiesFromColumnVisibility(group, topColumnVisibility));
                properties.putAll(elementConverter.getPropertiesFromValue(group, topValue));
                properties.putAll(elementConverter.getPropertiesFromTimestamp(group, topTimestamp));
                if (null == groupBy) {
                    if (null != schemaGroupBy) {
                        properties.remove(schemaGroupBy);
                    }
                } else {
                    properties.remove(groupBy);
                }
            } catch (final AccumuloElementConversionException e) {
                throw new RuntimeException(e);
            }

            return properties;
        }

        /**
         * unsupported
         *
         */
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private boolean areGroupByPropertiesEqual(final Key key1, final Key key2) {
            if (null != groupBy && groupBy.isEmpty()) {
                return true;
            }

            final byte[] colQual1 = key1.getColumnQualifierData().getBackingArray();
            final byte[] colQual2 = key2.getColumnQualifierData().getBackingArray();
            if (ByteUtils.areKeyBytesEqual(colQual1, colQual2)) {
                return true;
            }

            if (null == groupBy || groupBy.equals(schemaGroupBy)) {
                return false;
            }

            final BytesAndRange groupByPropBytes1;
            final BytesAndRange groupByPropBytes2;
            try {
                groupByPropBytes1 = elementConverter.getPropertiesAsBytesFromColumnQualifier(group, colQual1, groupBy.size());
                groupByPropBytes2 = elementConverter.getPropertiesAsBytesFromColumnQualifier(group, colQual2, groupBy.size());
            } catch (final AccumuloElementConversionException e) {
                throw new RuntimeException(e);
            }

            return ByteUtils.areKeyBytesEqual(groupByPropBytes1, groupByPropBytes2);
        }
    }

    @Override
    public Key getTopKey() {
        if (null == topKey) {
            return super.getTopKey();
        }

        return topKey;
    }

    @Override
    public Value getTopValue() {
        if (null == topKey) {
            return super.getTopValue();
        }

        return topValue;
    }

    @Override
    public boolean hasTop() {
        return null != topKey || super.hasTop();
    }

    @Override
    public void next() throws IOException {
        if (null != topKey) {
            topKey = null;
            topValue = null;
        } else {
            super.next();
        }

        findTop();
    }

    private final Key workKey = new Key();

    /**
     * Sets the topKey and topValue based on the top key of the source.
     */
    private void findTop() {
        // check if aggregation is needed
        if (super.hasTop()) {
            workKey.set(super.getTopKey());
            if (workKey.isDeleted()) {
                return;
            }

            final byte[] columnFamily = workKey.getColumnFamilyData().getBackingArray();
            final String group;
            try {
                group = elementConverter.getGroupFromColumnFamily(columnFamily);
            } catch (final AccumuloElementConversionException e) {
                throw new RuntimeException(e);
            }

            final ViewElementDefinition elementDef = view.getElement(group);
            Set<String> groupBy = elementDef.getGroupBy();
            if (null == groupBy) {
                groupBy = schema.getElement(group).getGroupBy();
            }

            final Iterator<Properties> iter = new KeyValueIterator(
                    getSource(), group, elementConverter, schema, groupBy);
            final Properties aggregatedProperties = reduce(group, workKey, iter, groupBy, elementDef.getAggregator());

            try {
                final Properties properties = elementConverter.getPropertiesFromColumnQualifier(group, workKey.getColumnQualifierData().getBackingArray());
                properties.putAll(elementConverter.getPropertiesFromColumnVisibility(group, workKey.getColumnVisibilityData().getBackingArray()));
                properties.putAll(aggregatedProperties);
                topValue = elementConverter.getValueFromProperties(group, properties);
                topKey = new Key(workKey.getRowData().getBackingArray(), columnFamily,
                        elementConverter.buildColumnQualifier(group, properties),
                        elementConverter.buildColumnVisibility(group, properties),
                        elementConverter.buildTimestamp(properties));
            } catch (final AccumuloElementConversionException e) {
                throw new RuntimeException(e);
            }

            while (iter.hasNext()) {
                iter.next();
            }
        }
    }

    @Override
    public void seek(final Range range, final Collection<ByteSequence> columnFamilies, final boolean inclusive)
            throws IOException {
        // do not want to seek to the middle of a value that should be
        // combined...

        final Range seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);

        super.seek(seekRange, columnFamilies, inclusive);
        findTop();

        if (null != range.getStartKey()) {
            while (hasTop() && getTopKey().equals(range.getStartKey(), PartialKey.ROW_COLFAM)
                    && getTopKey().getTimestamp() > range.getStartKey().getTimestamp()) {
                // The value has a more recent time stamp, so pass it up
                next();
            }

            while (hasTop() && range.beforeStartKey(getTopKey())) {
                next();
            }
        }
    }

    /**
     * Reduces an iterator of {@link Properties} into a single Properties object.
     *
     * @param group          the schema group taken from the key
     * @param key            The most recent version of the Key being reduced.
     * @param iter           An iterator over all {@link Properties} for different versions of the key.
     * @param groupBy        the groupBy properties
     * @param viewAggregator an optional view aggregator
     * @return The combined {@link Properties}.
     */
    public abstract Properties reduce(final String group, final Key key, final Iterator<Properties> iter, final Set<String> groupBy, final ElementAggregator viewAggregator);

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(final IteratorEnvironment env) {
        CoreKeyGroupByCombiner newInstance;
        try {
            newInstance = this.getClass().newInstance();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        newInstance.setSource(getSource().deepCopy(env));
        newInstance.schema = schema;
        newInstance.view = view;
        newInstance.elementConverter = elementConverter;
        return newInstance;
    }

    @Override
    public void init(final SortedKeyValueIterator<Key, Value> source, final Map<String, String> options, final IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        try {
            schema = Schema.fromJson(options.get(AccumuloStoreConstants.SCHEMA).getBytes(CommonConstants.UTF_8));
        } catch (final UnsupportedEncodingException e) {
            throw new SchemaException("Unable to deserialise the schema", e);
        }
        try {
            view = View.fromJson(options.get(AccumuloStoreConstants.VIEW).getBytes(CommonConstants.UTF_8));
        } catch (final UnsupportedEncodingException e) {
            throw new SchemaException("Unable to deserialise the view", e);
        }

        try {
            elementConverter = Class
                    .forName(options.get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS))
                    .asSubclass(AccumuloElementConverter.class)
                    .getConstructor(Schema.class)
                    .newInstance(schema);
        } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new AggregationException("Failed to load element converter from class name provided : "
                    + options.get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS), e);
        }
    }

    @Override
    public boolean validateOptions(final Map<String, String> options) {
        if (!options.containsKey(AccumuloStoreConstants.SCHEMA)) {
            throw new IllegalArgumentException("Must specify the " + AccumuloStoreConstants.SCHEMA);
        }
        if (!options.containsKey(AccumuloStoreConstants.VIEW)) {
            throw new IllegalArgumentException("Must specify the " + AccumuloStoreConstants.VIEW);
        }

        return true;
    }

    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptionsBuilder(AccumuloStoreConstants.COLUMN_QUALIFIER_AGGREGATOR_ITERATOR_NAME,
                "Applies a reduce function to a set of Properties with identical rowKey, column family and column qualifier constants.")
                .addSchemaNamedOption().build();
    }
}
