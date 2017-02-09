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

package uk.gov.gchq.gaffer.accumulostore.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.hash.Hash;

/**
 * Utilities for the creation of Bloom Filters
 */
public final class BloomFilterUtils {
    private BloomFilterUtils() {
        // private to prevent this class being instantiated.
        // All methods are static and should be called directly.
    }

    /**
     * Calculates the size of the
     * {@link org.apache.hadoop.util.bloom.BloomFilter} needed to achieve the
     * desired false positive rate given that the specified number of items will
     * be added to the set, but with the maximum size limited as specified.
     *
     * @param falsePositiveRate the false positive rate
     * @param numItemsToBeAdded the number of items to be added
     * @param maximumSize       the maximum size
     * @return An Integer representing the size of the bloom filter needed.
     */
    @SuppressFBWarnings(value = "ICAST_IDIV_CAST_TO_DOUBLE", justification = "the value is cast to an int after the division")
    public static int calculateBloomFilterSize(final double falsePositiveRate, final int numItemsToBeAdded,
            final int maximumSize) {
        final int size = (int) (-numItemsToBeAdded * Math.log(falsePositiveRate) / (Math.pow(Math.log(2.0), 2.0)));
        return Math.min(size, maximumSize);
    }

    /**
     * Calculates the optimal number of hash functions to use in a
     * {@link org.apache.hadoop.util.bloom.BloomFilter} of the given size, to
     * which the given number of items will be added.
     *
     * @param bloomFilterSize   the size of the bloom filter
     * @param numItemsToBeAdded the number of items to be added
     * @return An integer representing the optimal number of hashes to use
     */
    @SuppressFBWarnings(value = "ICAST_IDIV_CAST_TO_DOUBLE", justification = "the value is cast to an int after the division")
    public static int calculateNumHashes(final int bloomFilterSize, final int numItemsToBeAdded) {
        return Math.max(1, (int) ((bloomFilterSize / numItemsToBeAdded) * Math.log(2.0)));
    }

    /**
     * Returns a {@link org.apache.hadoop.util.bloom.BloomFilter} of the
     * necessary size to achieve the given false positive rate (subject to the
     * given maximum size), configured with the optimal number of hash
     * functions.
     *
     * @param falsePositiveRate the false positive rate
     * @param numItemsToBeAdded the number of items to be added
     * @param maximumSize       the maximum size
     * @return A new BloomFilter with the desired Settings
     */
    public static BloomFilter getBloomFilter(final double falsePositiveRate, final int numItemsToBeAdded,
            final int maximumSize) {
        final int size = calculateBloomFilterSize(falsePositiveRate, numItemsToBeAdded, maximumSize);
        final int numHashes = calculateNumHashes(size, numItemsToBeAdded);
        return new BloomFilter(size, numHashes, Hash.MURMUR_HASH);
    }

    /**
     * Returns a {@link org.apache.hadoop.util.bloom.BloomFilter} of the given
     * size.
     *
     * @param size the size of the bloom filter to create
     * @return A new BloomFilter of the desired size
     */
    public static BloomFilter getBloomFilter(final int size) {
        return new BloomFilter(size, 13, Hash.MURMUR_HASH);
    }
}
