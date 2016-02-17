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

package gaffer.accumulostore.key;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.key.exception.IteratorSettingException;
import gaffer.accumulostore.operation.AbstractRangeOperation;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.GetOperation;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.hadoop.util.bloom.BloomFilter;

/**
 * The iterator settings factory is designed to enable the AccumuloStore to
 * easily set all iterators that will be commonly required by different
 * implementations of the key. These methods may return null if the specified
 * iterator is not required/desired for your particular
 * {@link AccumuloKeyPackage} implementation.
 */
public interface IteratorSettingFactory {

    /**
     * Returns an {@link org.apache.accumulo.core.client.IteratorSetting} that
     * can be used to apply an iterator that will filter elements based on their
     * vertices membership in a given
     * {@link org.apache.hadoop.util.bloom.BloomFilter} to a
     * {@link org.apache.accumulo.core.client.Scanner}.
     *
     * @param filter the bloom filter
     * @return A new {@link IteratorSetting} for an Iterator capable of filtering elements based on checking its serialised form for membership in a {@link BloomFilter}
     * @throws IteratorSettingException if an iterator setting could not be created
     */
    IteratorSetting getBloomFilterIteratorSetting(final BloomFilter filter) throws IteratorSettingException;

    /**
     * Returns an {@link org.apache.accumulo.core.client.IteratorSetting} that
     * can be used to apply an iterator that will filter elements based on
     * predicates to a {@link org.apache.accumulo.core.client.Scanner}.
     *
     * @param view  the operation view
     * @param store the accumulo store
     * @return A new {@link IteratorSetting} for an Iterator capable of filtering {@link gaffer.data.element.Element}s based on a {@link View}
     * @throws IteratorSettingException if an iterator setting could not be created
     */
    IteratorSetting getElementFilterIteratorSetting(final View view, final AccumuloStore store)
            throws IteratorSettingException;

    /**
     * Returns an Iterator that will filter out
     * Edges/Entities/Undirected/Directed Edges based on the options in the
     * gaffer.accumulostore.operation May return null if this type of iterator
     * is not required for example if Key are constructed to enable this
     * filtering via the Accumulo Key
     *
     * @param operation the operation
     * @return A new {@link IteratorSetting} for an Iterator capable of filtering {@link gaffer.data.element.Element}s based on the options defined in the gaffer.accumulostore.operation
     */
    IteratorSetting getEdgeEntityDirectionFilterIteratorSetting(GetOperation<?, ?> operation);

    /**
     * Returns an Iterator that will aggregate values in the accumulo table,
     * this iterator will be applied to the table on creation
     *
     * @param store the accumulo store
     * @return A new {@link IteratorSetting} for an Iterator that will aggregate elements where they have the same key based on the {@link gaffer.data.elementdefinition.schema.DataSchema}
     * @throws IteratorSettingException if an iterator setting could not be created
     */
    IteratorSetting getAggregatorIteratorSetting(final AccumuloStore store) throws IteratorSettingException;

    /**
     * Returns an Iterator that will aggregate values at query time this is to
     * be used for the summarise option on getElement queries.
     *
     * @param store the accumulo store
     * @return A new {@link IteratorSetting} for an Iterator that will aggregate elements at query time on the {@link gaffer.data.elementdefinition.schema.DataSchema}
     * @throws IteratorSettingException if an iterator setting could not be created
     */
    IteratorSetting getQueryTimeAggregatorIteratorSetting(final AccumuloStore store) throws IteratorSettingException;

    /**
     * Returns an Iterator to be applied when doing
     * {@link AbstractRangeOperation} operations that will do any filtering of
     * Element properties that may have otherwise been done elsewhere e.g via
     * key creation An example of something that may not work correctly on
     * {@link AbstractRangeOperation} operations without this iterator is
     * Edges/Entities/Undirected/Directed Edges filtering May return null if
     * this type of iterator is not required for example if all needed filtering
     * is applied elsewhere.
     *
     * @param operation the operation to get the IteratorSetting for
     * @return A new {@link IteratorSetting} for an Iterator capable of
     * filtering {@link gaffer.data.element.Element}s based on the
     * options defined in the gaffer.accumulostore.operation
     */
    IteratorSetting getElementPropertyRangeQueryFilter(AbstractRangeOperation<?, ?> operation);

}
