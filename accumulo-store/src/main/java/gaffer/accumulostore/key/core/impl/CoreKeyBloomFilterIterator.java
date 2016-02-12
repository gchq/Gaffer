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
import gaffer.accumulostore.utils.ByteArrayEscapeUtils;
import gaffer.accumulostore.utils.Constants;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.util.bloom.BloomFilter;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * The BloomFilterIterator should filter out elements based on their membership of the provided bloomFilter.
 * This implementation may not work as desired depending on your gaffer.accumulostore.key implementation.
 */
public class CoreKeyBloomFilterIterator extends Filter {

    private BloomFilter filter;

    @Override
    public boolean accept(final Key key, final Value value) {
        return filter.membershipTest(new org.apache.hadoop.util.bloom.Key(getVertexFromKey(key.getRowData().getBackingArray())));
    }

    @Override
    public void init(final SortedKeyValueIterator<Key, Value> source, final Map<String, String> options, final IteratorEnvironment env) throws IOException {
        validateOptions(options);
        super.init(source, options, env);
    }

    @Override
    public boolean validateOptions(final Map<String, String> options) {
        if (!options.containsKey(Constants.BLOOM_FILTER)) {
            throw new BloomFilterIteratorException("Must set the " + Constants.BLOOM_FILTER + " option");
        }
        filter = new BloomFilter();
        try {
            filter.readFields(new DataInputStream(new ByteArrayInputStream(
                    options.get(Constants.BLOOM_FILTER).getBytes(Constants.BLOOM_FILTER_CHARSET))));
        } catch (final IOException e) {
            throw new BloomFilterIteratorException("Failed to re-create serialised bloom filter", e);
        }
        return true;
    }

    @Override
    public IteratorOptions describeOptions() {
        final Map<String, String> namedOptions = new HashMap<>();
        return new IteratorOptions(Constants.BLOOM_FILTER,
                "Required The serialised form of the bloom filter that keys will be tested against ",
                namedOptions, null);
    }

    public byte[] getVertexFromKey(final byte[] key) {
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
}
