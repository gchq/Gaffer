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
import gaffer.accumulostore.key.core.impl.model.ColumnQualifierColumnVisibilityValueTriple;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.accumulostore.utils.IteratorOptionsBuilder;
import gaffer.data.elementdefinition.schema.exception.SchemaException;
import gaffer.store.schema.StoreSchema;
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
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A copy of Accumulo {@link org.apache.accumulo.core.iterators.Combiner} but
 * combining values with identical rowKey and column family.
 * <p>
 * Users extending this class must specify a reduce() method.
 */
public abstract class CoreKeyColumnQualifierColumnVisibilityValueCombiner extends WrappingIterator
        implements OptionDescriber {
    protected StoreSchema storeSchema;

    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "elementConverter is initialised in init method, which is always called prior to this method")
    protected AccumuloElementConverter elementConverter;

    /**
     * A Java Iterator that iterates over the
     * {@link ColumnQualifierColumnVisibilityValueTriple} for a given row Key
     * and column family from a source {@link SortedKeyValueIterator}.
     */
    public static class ColumnQualifierColumnVisibilityValueTripleIterator
            implements Iterator<ColumnQualifierColumnVisibilityValueTriple> {
        final Key columnQualifierColumnVisibilityValueTripleIteratorTopKey;
        final SortedKeyValueIterator<Key, Value> source;
        boolean hasNext;

        /**
         * Constructs an iterator over {@link Value}s whose {@link Key}s are
         * versions of the current topKey of the source
         * {@link SortedKeyValueIterator}.
         *
         * @param source The {@link SortedKeyValueIterator} of {@link Key},
         *               {@link Value} pairs from which to read data.
         */
        public ColumnQualifierColumnVisibilityValueTripleIterator(final SortedKeyValueIterator<Key, Value> source) {
            this.source = source;
            final Key unsafeRef = source.getTopKey();
            columnQualifierColumnVisibilityValueTripleIteratorTopKey = new Key(unsafeRef.getRow().getBytes(),
                    unsafeRef.getColumnFamily().getBytes(), unsafeRef.getColumnQualifier().getBytes(),
                    unsafeRef.getColumnVisibility().getBytes(), unsafeRef.getTimestamp(), unsafeRef.isDeleted(), true);
            hasNext = _hasNext();
        }

        private boolean _hasNext() {
            return source.hasTop() && !source.getTopKey().isDeleted()
                    && columnQualifierColumnVisibilityValueTripleIteratorTopKey.equals(source.getTopKey(),
                    PartialKey.ROW_COLFAM);
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
        public ColumnQualifierColumnVisibilityValueTriple next() {
            if (!hasNext) {
                throw new NoSuchElementException();
            }

            final byte[] topColumnQualifier = source.getTopKey().getColumnQualifierData().getBackingArray();
            final byte[] topColumnVisibility = source.getTopKey().getColumnVisibilityData().getBackingArray();
            final Value topValue = new Value(source.getTopValue());

            try {
                source.next();
                hasNext = _hasNext();
            } catch (final IOException e) {
                throw new RuntimeException(e); // Looks like a bad idea, but
                // this is what the in-built
                // Combiner iterator does
            }
            final ColumnQualifierColumnVisibilityValueTriple topVisValPair = new ColumnQualifierColumnVisibilityValueTriple(
                    topColumnQualifier, topColumnVisibility, topValue);
            return topVisValPair;
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
    }

    Key topKey;
    Value topValue;
    ColumnQualifierColumnVisibilityValueTriple topVisValPair;

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

            final Iterator<ColumnQualifierColumnVisibilityValueTriple> iter = new ColumnQualifierColumnVisibilityValueTripleIterator(
                    getSource());
            topVisValPair = reduce(workKey, iter);
            topValue = topVisValPair.getValue();
            topKey = new Key(workKey.getRowData().getBackingArray(), workKey.getColumnFamilyData().getBackingArray(),
                    topVisValPair.getColumnQualifier(), topVisValPair.getColumnVisibility(), workKey.getTimestamp());

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
     * Reduces a list of triples of (column qualifier, column visibility, Value)
     * into a single triple.
     *
     * @param key  The most recent version of the Key being reduced.
     * @param iter An iterator over the Values for different versions of the key.
     * @return The combined Value.
     */
    public abstract ColumnQualifierColumnVisibilityValueTriple reduce(final Key key,
                                                                      final Iterator<ColumnQualifierColumnVisibilityValueTriple> iter);

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(final IteratorEnvironment env) {
        CoreKeyColumnQualifierColumnVisibilityValueCombiner newInstance;
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
        if (!options.containsKey(AccumuloStoreConstants.STORE_SCHEMA)) {
            throw new IllegalArgumentException("Must specify the " + AccumuloStoreConstants.STORE_SCHEMA);
        }
        try {
            storeSchema = StoreSchema.fromJson(options.get(AccumuloStoreConstants.STORE_SCHEMA).getBytes(AccumuloStoreConstants.UTF_8_CHARSET));
        } catch (final UnsupportedEncodingException e) {
            throw new SchemaException("Unable to deserialise the store schema", e);
        }
        return true;
    }

    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptionsBuilder(AccumuloStoreConstants.QUERY_TIME_AGGREGATION_ITERATOR_NAME,
                "Applies a reduce function to triples of (column qualifier, column visibility, value) with identical (rowKey, column family)")
                .addStoreSchemaNamedOption().build();
    }
}
