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

import gaffer.accumulostore.key.exception.BloomFilterIteratorException;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.accumulostore.utils.ByteArrayEscapeUtils;
import gaffer.accumulostore.utils.IteratorOptionsBuilder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.util.bloom.BloomFilter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Map;

/**
 * The BloomFilterIterator should filter out elements based on their membership
 * of the provided bloomFilter. This implementation may not work as desired
 * depending on your {@link gaffer.accumulostore.key.AccumuloKeyPackage}
 * implementation.
 */
public class CoreKeyBloomFilterIterator extends Filter {

    private BloomFilter filter;

    @Override
    public boolean accept(final Key key, final Value value) {
        return filter.membershipTest(
                new org.apache.hadoop.util.bloom.Key(getVertexFromKey(key.getRowData().getBackingArray())));
    }

    protected byte[] getVertexFromKey(final byte[] key) {
        int pos = -1;
        for (int i = 0; i < key.length; ++i) {
            if (key[i] == ByteArrayEscapeUtils.DELIMITER) {
                pos = i;
                break;
            }
        }
        if (pos != -1) {
            return Arrays.copyOf(key, pos);
        } else {
            return key;
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
        if (!options.containsKey(AccumuloStoreConstants.BLOOM_FILTER)) {
            throw new BloomFilterIteratorException("Must set the " + AccumuloStoreConstants.BLOOM_FILTER + " option");
        }
        filter = new BloomFilter();
        final byte[] bytes;
        try {
            bytes = options.get(AccumuloStoreConstants.BLOOM_FILTER).getBytes(AccumuloStoreConstants.BLOOM_FILTER_CHARSET);
        } catch (UnsupportedEncodingException e) {
            throw new BloomFilterIteratorException("Failed to re-create serialised bloom filter", e);
        }

        try (final InputStream inStream = new ByteArrayInputStream(bytes);
             final DataInputStream dataStream = new DataInputStream(inStream)) {
            filter.readFields(dataStream);
        } catch (final IOException e) {
            throw new BloomFilterIteratorException("Failed to re-create serialised bloom filter", e);
        }
        return true;
    }

    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptionsBuilder(AccumuloStoreConstants.BLOOM_FILTER_ITERATOR_NAME, "Bloom Filter")
                .addNamedOption(AccumuloStoreConstants.BLOOM_FILTER,
                        "Required: The serialised form of the bloom filter that keys will be tested against")
                .build();
    }

}
