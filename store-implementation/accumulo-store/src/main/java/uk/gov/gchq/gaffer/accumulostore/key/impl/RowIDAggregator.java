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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

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
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class RowIDAggregator extends WrappingIterator implements OptionDescriber {

    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "schema is initialised in validateOptions method, which is always called first")
    protected Schema schema = null;
    @SuppressFBWarnings({"UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", "UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD"})
    protected AccumuloElementConverter elementConverter = null;
    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "aggregator is initialised in init method, which is always called first")
    private ElementAggregator aggregator = null;
    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "group is initialised in init method, which is always called first")
    private String group = null;

    private SortedKeyValueIterator<Key, Value> source;

    private Key workKey;
    private Key topKey;
    private Value topValue;

    private Range currentRange;
    private Collection<ByteSequence> currentColumnFamilies;
    private boolean currentColumnFamiliesInclusive;

    public RowIDAggregator() {
        topKey = null;
    }

    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptionsBuilder(AccumuloStoreConstants.COLUMN_QUALIFIER_AGGREGATOR_ITERATOR_NAME,
                "Applies a findTop function to triples of (column qualifier, column visibility, value) with identical (rowKey, column family)")
                .addSchemaNamedOption().addElementConverterClassNamedOption().build();
    }

    @Override
    public boolean validateOptions(final Map<String, String> options) {
        if (!options.containsKey(AccumuloStoreConstants.SCHEMA)) {
            throw new IllegalArgumentException("Must specify the " + AccumuloStoreConstants.SCHEMA);
        }
        if (!options.containsKey(AccumuloStoreConstants.COLUMN_FAMILY)) {
            throw new IllegalArgumentException("Must specify the " + AccumuloStoreConstants.COLUMN_FAMILY);
        }
        return true;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(final IteratorEnvironment env) {
        RowIDAggregator rowIDAggregator = new RowIDAggregator();
        rowIDAggregator.topKey = this.topKey;
        rowIDAggregator.topValue = this.topValue;
        rowIDAggregator.schema = this.schema;
        rowIDAggregator.aggregator = this.aggregator;
        rowIDAggregator.elementConverter = this.elementConverter;
        Key newWorkKey = new Key();
        newWorkKey.set(workKey);
        rowIDAggregator.workKey = newWorkKey;
        return rowIDAggregator;
    }

    @Override
    public void init(final SortedKeyValueIterator<Key, Value> source, final Map<String, String> options, final IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        this.source = source;
        try {
            schema = Schema.fromJson(options.get(AccumuloStoreConstants.SCHEMA).getBytes(CommonConstants.UTF_8));
        } catch (final UnsupportedEncodingException e) {
            throw new SchemaException("Unable to deserialise the schema", e);
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

        group = options.get(AccumuloStoreConstants.COLUMN_FAMILY);
        aggregator = schema.getElement(group).getFullAggregator();
    }

    @Override
    public void seek(final Range range, final Collection<ByteSequence> columnFamilies, final boolean inclusive) throws IOException {
        topKey = null;
        workKey = new Key();
        super.seek(range, columnFamilies, inclusive);
        currentRange = range;
        currentColumnFamilies = columnFamilies;
        currentColumnFamiliesInclusive = inclusive;
        findTop();
    }

    @Override
    public void next() throws IOException {
        topKey = null;
        workKey = new Key();
        if (!source.hasTop()) {
            return;
        }
        source.next();
        findTop();
    }

    @Override
    public Key getTopKey() {
        return topKey;
    }

    @Override
    public Value getTopValue() {
        return topValue;
    }

    @Override
    public boolean hasTop() {
        return null != topKey;
    }

    /**
     * Given the current position in the source}, filter to only the columns specified. Sets topKey and topValue to non-null on success
     *
     * @throws IOException Failure to seek
     */
    protected void findTop() throws IOException {
        if (!source.hasTop()) {
            return;
        }
        PropertiesIterator iter = new PropertiesIterator(source, currentRange, currentColumnFamilies, currentColumnFamiliesInclusive, group, workKey, elementConverter);
        Properties topProperties = reduce(iter);
        try {
            topValue = elementConverter.getValueFromProperties(group, topProperties);
            topKey = new Key(workKey.getRowData().getBackingArray(), group.getBytes(CommonConstants.UTF_8),
                    elementConverter.buildColumnQualifier(group, topProperties),
                    elementConverter.buildColumnVisibility(group, topProperties),
                    elementConverter.buildTimestamp(topProperties));
        } catch (final AccumuloElementConversionException e) {
            throw new RuntimeException(e);
        }
    }

    private Properties reduce(final Iterator<Properties> iter) {
        Properties state = null;
        Properties properties;
        while (iter.hasNext()) {
            properties = iter.next();
            if (null != properties) {
                state = aggregator.apply(state, properties);
            }
        }

        return state;
    }

    public static class PropertiesIterator implements Iterator<Properties> {

        private final Range currentRange;
        private final SortedKeyValueIterator<Key, Value> source;
        private final Collection<ByteSequence> currentColumnFamilies;
        private final boolean currentColumnFamiliesInclusive;
        private final AccumuloElementConverter elementConverter;
        private Key currentKey;
        private Value currentValue;
        private Key workKeyRef;
        private String group;

        public PropertiesIterator(final SortedKeyValueIterator<Key, Value> source, final Range currentRange, final Collection<ByteSequence> currentColumnFamilies, final boolean currentColumnFamiliesInclusive, final String group, final Key workKeyRef, final AccumuloElementConverter elementConverter) throws IOException {
            this.source = source;
            this.currentColumnFamilies = currentColumnFamilies;
            this.currentColumnFamiliesInclusive = currentColumnFamiliesInclusive;
            this.currentRange = currentRange;
            this.workKeyRef = workKeyRef;
            this.group = group;
            this.elementConverter = elementConverter;
        }

        @Override
        public boolean hasNext() {
            if (!source.hasTop()) {
                return false;
            }
            currentKey = source.getTopKey();
            currentValue = source.getTopValue();
            if (currentRange.afterEndKey(currentKey)) {
                return false;
            }
            final String currentColumnFamily;
            try {
                currentColumnFamily = new String(source.getTopKey().getColumnFamilyData().getBackingArray(), CommonConstants.UTF_8);
            } catch (final UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
            if (group.equals(currentColumnFamily) && !source.getTopKey().isDeleted()) {
                return true;
            }
            final Key nextKey = currentKey.followingKey(PartialKey.ROW);
            try {
                source.seek(new Range(nextKey, true, currentRange.getEndKey(), currentRange.isEndKeyInclusive()), currentColumnFamilies, currentColumnFamiliesInclusive);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            return hasNext();
        }

        @Override
        public Properties next() {
            return nextRecordFound(currentKey, currentValue);
        }

        private Properties nextRecordFound(final Key key, final Value value) {
            this.workKeyRef.set(key);
            try {
                source.next();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            final Properties properties;
            try {
                properties = elementConverter.getPropertiesFromColumnQualifier(group, key.getColumnQualifierData().getBackingArray());
                properties.putAll(elementConverter.getPropertiesFromColumnVisibility(group, key.getColumnVisibilityData().getBackingArray()));
                properties.putAll(elementConverter.getPropertiesFromTimestamp(group, key.getTimestamp()));
                properties.putAll(elementConverter.getPropertiesFromValue(group, value));
            } catch (final AccumuloElementConversionException e) {
                throw new RuntimeException(e);
            }

            return properties;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
