/**
 * Copyright 2015 Crown Copyright
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
package gaffer.utils;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.hash.Hash;

/**
 * Utility functions for creating {@link BloomFilter}s.
 */
public class BloomFilterUtilities {

    private BloomFilterUtilities() { }

    /**
     * Calculates the size of the {@link BloomFilter} needed to achieve the desired false positive rate given that the
     * specified number of items will be added to the set, but with the maximum size limited as specified.
     *
     * @param falsePositiveRate  The desired false positive rate
     * @param numItemsToBeAdded  The number of items that will be added to the set
     * @param maximumSize  The maximum size of the BloomFilter
     * @return The best size for the BloomFilter
     */
    public static int calculateBloomFilterSize(double falsePositiveRate, int numItemsToBeAdded, int maximumSize) {
        int size = (int) (-numItemsToBeAdded * Math.log(falsePositiveRate) / (Math.pow(Math.log(2.0), 2.0)));
        return Math.min(size, maximumSize);
    }

    /**
     * Calculates the optimal number of hash functions to use in a {@link BloomFilter} of the given size, to which the
     * given number of items will be added.
     *
     * @param bloomFilterSize  The size of the BloomFilter
     * @param numItemsToBeAdded  The number of items to be added to the BloomFilter
     * @return The optimal number of hashes
     */
    public static int calculateNumHashes(int bloomFilterSize, int numItemsToBeAdded) {
        return Math.max(1, (int) ((bloomFilterSize / numItemsToBeAdded) * Math.log(2.0)));
    }

    /**
     * Returns a {@link BloomFilter} of the necessary size to achieve the given false positive rate (subject
     * to the given maximum size), configured with the optimal number of hash functions.
     *
     * @param falsePositiveRate  The desired false positive rate
     * @param numItemsToBeAdded  The number of items to be added to the BloomFilter
     * @param maximumSize  The maximum size of the BloomFilter
     * @return A BloomFilter with the specified properties
     */
    public static BloomFilter getBloomFilter(double falsePositiveRate, int numItemsToBeAdded, int maximumSize) {
        int size = calculateBloomFilterSize(falsePositiveRate, numItemsToBeAdded, maximumSize);
        int numHashes = calculateNumHashes(size, numItemsToBeAdded);
        return new BloomFilter(size, numHashes, Hash.MURMUR_HASH);
    }

    /**
     * Returns a {@link BloomFilter} of the given size.
     *
     * @param size  The desired size
     * @return A BloomFilter with the given size
     */
    public static BloomFilter getBloomFilter(int size) {
        return new BloomFilter(size, 13, Hash.MURMUR_HASH);
    }
}
