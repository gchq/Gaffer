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
package gaffer.accumulostore.key.core.impl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gaffer.accumulostore.key.AccumuloElementConverter;
import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.accumulostore.utils.ByteUtils;
import gaffer.accumulostore.utils.IteratorOptionsBuilder;
import gaffer.accumulostore.utils.StorePositions;
import gaffer.commonutil.CommonConstants;
import gaffer.data.element.Properties;
import gaffer.data.elementdefinition.exception.SchemaException;
import gaffer.data.elementdefinition.view.View;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaElementDefinition;
import gaffer.store.schema.TypeDefinition;
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
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A copy of Accumulo {@link org.apache.accumulo.core.iterators.Combiner} but
 * combining values with identical rowKey and column family.
 * <p>
 * Users extending this class must specify a reduce() method.
 */
public abstract class CoreKeyValueCombiner extends WrappingIterator
        implements OptionDescriber {
    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "schema is initialised in validateOptions method, which is always called first")
    protected Schema schema;

    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "view is initialised in init method, which is always called first")
    protected View view;

    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "elementConverter is initialised in init method, which is always called first")
    protected AccumuloElementConverter elementConverter;

    /**
     * A Java Iterator that iterates over the properties for a given row Key
     * and column family from a source {@link SortedKeyValueIterator}.
     */
    public static class KeyValueIterator implements Iterator<Properties> {
        private final Key topKey;
        private final String group;
        private final SortedKeyValueIterator<Key, Value> source;
        private final AccumuloElementConverter elementConverter;
        private final SchemaElementDefinition schemaElementDefinition;
        private final List<String> groupByProperties;
        private boolean hasNext;

        /**
         * Constructs an iterator over {@link Value}s whose {@link Key}s are
         * versions of the current topKey of the source
         * {@link SortedKeyValueIterator}.
         *
         * @param source            The {@link SortedKeyValueIterator} of {@link Key},
         *                          {@link Value} pairs from which to read data.
         * @param elementConverter  the elementConverter to use
         * @param schema            the store schema
         * @param groupByProperties the properties to group by - if null then don't group by any properties.
         */
        public KeyValueIterator(final SortedKeyValueIterator<Key, Value> source,
                                final AccumuloElementConverter elementConverter,
                                final Schema schema, final List<String> groupByProperties) {
            this.source = source;
            this.elementConverter = elementConverter;
            final Key unsafeRef = source.getTopKey();
            topKey = new Key(unsafeRef.getRow().getBytes(),
                    unsafeRef.getColumnFamily().getBytes(), unsafeRef.getColumnQualifier().getBytes(),
                    unsafeRef.getColumnVisibility().getBytes(), unsafeRef.getTimestamp(), unsafeRef.isDeleted(), true);

            try {
                group = elementConverter.getGroupFromColumnFamily(topKey.getColumnFamilyData().getBackingArray());
            } catch (AccumuloElementConversionException e) {
                throw new RuntimeException(e);
            }

            schemaElementDefinition = schema.getElement(group);
            this.groupByProperties = groupByProperties;

            hasNext = _hasNext();
        }

        private boolean _hasNext() {
            return source.hasTop() && !source.getTopKey().isDeleted()
                    && topKey.equals(source.getTopKey(), PartialKey.ROW_COLFAM)
                    && checkGroupByProperties(topKey, source.getTopKey());
        }

        /**
         * @return <code>true</code> if there is another Value
         * @see java.util.Iterator#hasNext()
         */
        @Override
        public boolean hasNext() {
            return hasNext;
        }

        /**
         * @return the next Value
         * @see java.util.Iterator#next()
         */
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
                // this is what the in-built
                // Combiner iterator does
            }

            final Properties properties = new Properties();
            try {
                properties.putAll(elementConverter.getPropertiesFromColumnQualifier(group, topColumnQualifier));
                properties.putAll(elementConverter.getPropertiesFromColumnVisibility(group, topColumnVisibility));
                properties.putAll(elementConverter.getPropertiesFromValue(group, topValue));
                properties.putAll(elementConverter.getPropertiesFromTimestamp(group, topTimestamp));
                if (null != groupByProperties) {
                    properties.remove(groupByProperties);
                }
            } catch (final AccumuloElementConversionException e) {
                throw new RuntimeException(e);
            }

            return properties;
        }

        /**
         * unsupported
         *
         * @see java.util.Iterator#remove()
         */
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private boolean checkGroupByProperties(final Key key1, final Key key2) {
            if (null == groupByProperties) {
                return false;
            }

            if (groupByProperties.isEmpty()) {
                return true;
            }

            List<String> colQualProps = null;
            for (String property : groupByProperties) {
                final TypeDefinition propertyTypeDef = schemaElementDefinition.getPropertyTypeDef(property);
                if (null != propertyTypeDef) {
                    final String position = propertyTypeDef.getPosition();
                    if (StorePositions.VISIBILITY.isEqual(position)) {
                        if (!ByteUtils.areKeyBytesEqual(key1.getColumnVisibilityData().getBackingArray(), key2.getColumnVisibilityData().getBackingArray())) {
                            return false;
                        }
                    } else if (StorePositions.COLUMN_QUALIFIER.isEqual(position)) {
                        if (null == colQualProps) {
                            colQualProps = new ArrayList<>();
                        }
                        colQualProps.add(property);
                    } else {
                        throw new IllegalArgumentException("Only properties with position " + StorePositions.COLUMN_QUALIFIER.name()
                                + " or " + StorePositions.VISIBILITY.name() + " can be include in the groupByProperties.");
                    }
                }
            }

            if (null != colQualProps) {
                final byte[] colQual1 = key1.getColumnQualifierData().getBackingArray();
                final byte[] colQual2 = key2.getColumnQualifierData().getBackingArray();
                if (ByteUtils.areKeyBytesEqual(colQual1, colQual2)) {
                    return true;
                }

                final List<byte[]> propBytes1;
                final List<byte[]> propBytes2;
                try {
                    propBytes1 = elementConverter.getPropertyBytesFromColumnQualifier(schemaElementDefinition, colQual1, colQualProps);
                    propBytes2 = elementConverter.getPropertyBytesFromColumnQualifier(schemaElementDefinition, colQual2, colQualProps);
                } catch (AccumuloElementConversionException e) {
                    throw new RuntimeException(e);
                }

                if (propBytes1.size() != propBytes2.size()) {
                    return false;
                }

                final Iterator<byte[]> itr1 = propBytes1.iterator();
                final Iterator<byte[]> itr2 = propBytes2.iterator();
                while (itr1.hasNext()) {
                    if (!ByteUtils.areKeyBytesEqual(itr1.next(), itr2.next())) {
                        return false;
                    }
                }
            }

            return true;
        }
    }

    Key topKey;
    Value topValue;

    @Override
    public Key getTopKey() {
        if (topKey == null) {
            return super.getTopKey();
        }

        return topKey;
    }

    @Override
    public Value getTopValue() {
        if (topKey == null) {
            return super.getTopValue();
        }

        return topValue;
    }

    @Override
    public boolean hasTop() {
        return topKey != null || super.hasTop();
    }

    @Override
    public void next() throws IOException {
        if (topKey != null) {
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

            final byte[] colFam = workKey.getColumnFamilyData().getBackingArray();

            final String group;
            try {
                group = elementConverter.getGroupFromColumnFamily(colFam);
            } catch (AccumuloElementConversionException e) {
                throw new RuntimeException(e);
            }

            final Iterator<Properties> iter = new KeyValueIterator(
                    getSource(), elementConverter, schema, view.getGroupByProperties());
            final Properties topProperties = reduce(group, workKey, iter);


            final byte[] colQual;
            final byte[] vis;
            final long timestamp;
            final Properties aggregatedProperties;
            try {
                aggregatedProperties = elementConverter.getPropertiesFromColumnQualifier(group, workKey.getColumnQualifierData().getBackingArray());
                aggregatedProperties.putAll(elementConverter.getPropertiesFromColumnVisibility(group, workKey.getColumnVisibilityData().getBackingArray()));

                // Remove any group by properties in case they override the aggregated properties
                topProperties.remove(view.getGroupByProperties());
                aggregatedProperties.putAll(topProperties);

                colQual = elementConverter.buildColumnQualifier(group, aggregatedProperties);
                vis = elementConverter.buildColumnVisibility(group, aggregatedProperties);
                timestamp = elementConverter.buildTimestamp(group, aggregatedProperties, workKey.getTimestamp());
                topValue = elementConverter.getValueFromProperties(aggregatedProperties, group);
            } catch (AccumuloElementConversionException e) {
                throw new RuntimeException(e);
            }

            topKey = new Key(workKey.getRowData().getBackingArray(), colFam, colQual, vis, timestamp);

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

        if (range.getStartKey() != null) {
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
     * @param group the schema group taken from the key
     * @param key   The most recent version of the Key being reduced.
     * @param iter  An iterator over all {@link Properties} for different versions of the key.
     * @return The combined {@link Properties}.
     */
    public abstract Properties reduce(final String group, final Key key, final Iterator<Properties> iter);

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(final IteratorEnvironment env) {
        CoreKeyValueCombiner newInstance;
        try {
            newInstance = this.getClass().newInstance();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        newInstance.setSource(getSource().deepCopy(env));
        return newInstance;
    }

    @Override
    public void init(final SortedKeyValueIterator<Key, Value> source, final Map<String, String> options,
                     final IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
    }

    @Override
    public boolean validateOptions(final Map<String, String> options) {
        if (!options.containsKey(AccumuloStoreConstants.SCHEMA)) {
            throw new IllegalArgumentException("Must specify the " + AccumuloStoreConstants.SCHEMA);
        }
        try {
            schema = Schema.fromJson(options.get(AccumuloStoreConstants.SCHEMA).getBytes(CommonConstants.UTF_8));
        } catch (final UnsupportedEncodingException e) {
            throw new SchemaException("Unable to deserialise the schema", e);
        }

        if (!options.containsKey(AccumuloStoreConstants.VIEW)) {
            throw new IllegalArgumentException("Must specify the " + AccumuloStoreConstants.VIEW);
        }
        try {
            view = View.fromJson(options.get(AccumuloStoreConstants.VIEW).getBytes(CommonConstants.UTF_8));
        } catch (final UnsupportedEncodingException e) {
            throw new SchemaException("Unable to deserialise the view", e);
        }

        if (!view.isSummarise()) {
            throw new IllegalArgumentException("This combiner should only be used with a view that requires summarising.");
        }

        return true;
    }

    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptionsBuilder(AccumuloStoreConstants.QUERY_TIME_AGGREGATION_ITERATOR_NAME,
                "Applies a reduce function to a set of Properties with identical rowKey, column family and column qualifier constants.")
                .addSchemaNamedOption().build();
    }
}
