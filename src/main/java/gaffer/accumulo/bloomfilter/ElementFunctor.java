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
package gaffer.accumulo.bloomfilter;

import gaffer.accumulo.AccumuloBackedGraph;
import gaffer.accumulo.ConversionUtils;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.TypeValue;

import java.io.IOException;
import java.util.Arrays;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.keyfunctor.KeyFunctor;

/**
 * Used to add BloomFilters to the table of {@link Entity}s and {@link Edge}s
 * so that if a range involving just one {@link TypeValue} is queried for
 * (as happens when we use {@link AccumuloBackedGraph} to retrieve data) then
 * the BloomFilter can be used to speed up the query if the entity is not present.
 */
public class ElementFunctor implements KeyFunctor {

	/**
	 * Transforms a {@link Range} into a BloomFilter key. If the first type-values in the
	 * start and end keys of the range are the same, then we can create the
	 * appropriate BloomFilter key. If the key does not correspond to either
	 * an {@link Entity} or an {@link Edge} then an {@link IOException}
	 * will be thrown by the {@link ConversionUtils#getFirstTypeValue} method which will be
	 * caught and then <code>null</code> is returned. Otherwise, we return <code>null</code>
	 * to indicate that the range cannot be converted into a single key for the Bloom filter.
	 *
	 * @param range  The range to be transformed
	 * @return The Bloom filter key or <code>null</code> if no single key can be created.
	 */
	@Override
	public org.apache.hadoop.util.bloom.Key transform(Range range) {
		if (range.getStartKey() == null || range.getEndKey() == null) {
			return null;
		}
		try {
			byte[] startKeyFirstTypeValue = ConversionUtils.getFirstTypeValue(range.getStartKey());
			byte[] endKeyFirstTypeValue = ConversionUtils.getFirstTypeValue(range.getEndKey());
			if (Arrays.equals(startKeyFirstTypeValue, endKeyFirstTypeValue)) {
				return new org.apache.hadoop.util.bloom.Key(startKeyFirstTypeValue);
			}
		} catch (IOException e) {
			return null;
		}
		return null;
	}

	/**
	 * Transforms an Accumulo {@link Key} into the corresponding key for the Bloom
	 * filter. If the key does not correspond to either an {@link Entity} or an
	 * {@link Edge} then an {@link IOException} will be thrown by the
	 * {@link ConversionUtils#getFirstTypeValue} method which will be caught and then
	 * <code>null</code> is returned.
	 *
	 * @param key  The Accumulo key to be transformed
	 * @return The Bloom filter key or <code>null</code> if the key does not correspond to an {@link Entity}
	 * or {@link Edge}
	 */
	@Override
	public org.apache.hadoop.util.bloom.Key transform(org.apache.accumulo.core.data.Key key) {
		try {
			return new org.apache.hadoop.util.bloom.Key(ConversionUtils.getFirstTypeValue(key));
		} catch (IOException e) {
			return null;
		}
	}

}
