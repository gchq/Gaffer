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

package uk.gov.gchq.gaffer.accumulostore.key;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.hadoop.util.bloom.BloomFilter;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.GetElementsOperation;

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
     * predicates specified in the preAggregation block in the view to a {@link org.apache.accumulo.core.client.Scanner}.
     *
     * @param view  the operation view
     * @param store the accumulo store
     * @return A new {@link IteratorSetting} for an Iterator capable of filtering {@link uk.gov.gchq.gaffer.data.element.Element}s based on a {@link View}
     * @throws IteratorSettingException if an iterator setting could not be created
     */
    IteratorSetting getElementPreAggregationFilterIteratorSetting(final View view, final AccumuloStore store)
            throws IteratorSettingException;

    /**
     * Returns an {@link org.apache.accumulo.core.client.IteratorSetting} that
     * can be used to apply an iterator that will filter elements based on
     * predicates specified in the postAggregation block in the view to a {@link org.apache.accumulo.core.client.Scanner}.
     *
     * @param view  the operation view
     * @param store the accumulo store
     * @return A new {@link IteratorSetting} for an Iterator capable of filtering {@link uk.gov.gchq.gaffer.data.element.Element}s based on a {@link View}
     * @throws IteratorSettingException if an iterator setting could not be created
     */
    IteratorSetting getElementPostAggregationFilterIteratorSetting(final View view, final AccumuloStore store)
            throws IteratorSettingException;

    /**
     * Returns an Iterator that will filter out
     * Edges/Entities/Undirected/Directed Edges based on the options in the
     * gaffer.accumulostore.operation May return null if this type of iterator
     * is not required for example if Key are constructed to enable this
     * filtering via the Accumulo Key
     *
     * @param operation the operation
     * @return A new {@link IteratorSetting} for an Iterator capable of filtering {@link uk.gov.gchq.gaffer.data.element.Element}s based on the options defined in the gaffer.accumulostore.operation
     */
    IteratorSetting getEdgeEntityDirectionFilterIteratorSetting(final GetElementsOperation<?, ?> operation);

    /**
     * Returns an Iterator that will aggregate values in the accumulo table,
     * this iterator will be applied to the table on creation
     *
     * @param store the accumulo store
     * @return A new {@link IteratorSetting} for an Iterator that will aggregate elements where they have the same key based on the {@link uk.gov.gchq.gaffer.store.schema.Schema}
     * @throws IteratorSettingException if an iterator setting could not be created
     */
    IteratorSetting getAggregatorIteratorSetting(final AccumuloStore store) throws IteratorSettingException;

    /**
     * Returns an Iterator that will validate elements in the accumulo table based
     * on the validator provided in the {@link uk.gov.gchq.gaffer.store.schema.Schema}
     * this iterator will be applied to the table on creation
     *
     * @param store the accumulo store
     * @return A new {@link IteratorSetting} for an Iterator that will validate elements
     */
    IteratorSetting getValidatorIteratorSetting(final AccumuloStore store);

    /**
     * Returns an Iterator that will aggregate values at query time this is to
     * be used for the summarise option on getElement queries.
     *
     * @param view  the operation view
     * @param store the accumulo store
     * @return A new {@link IteratorSetting} for an Iterator that will aggregate elements at query time on the {@link uk.gov.gchq.gaffer.store.schema.Schema}
     * @throws IteratorSettingException if an iterator setting could not be created
     */
    IteratorSetting getQueryTimeAggregatorIteratorSetting(final View view, final AccumuloStore store) throws IteratorSettingException;

    /**
     * Returns an Iterator that will aggregate properties across a range of RowID's for a given columnFamily
     *
     * @param store the accumulo store
     * @param columnFamily the columnFamily that will be summarised
     * @return A new {@link IteratorSetting} for an Iterator that will aggregate elements at query time on the {@link uk.gov.gchq.gaffer.store.schema.Schema}
     * @throws IteratorSettingException if an iterator setting could not be created
     */
    IteratorSetting getRowIDAggregatorIteratorSetting(final AccumuloStore store, final String columnFamily) throws IteratorSettingException;

    /**
     * Returns an Iterator to be applied when doing range operations that will do any filtering of
     * Element properties that may have otherwise been done elsewhere e.g via
     * key creation.  Examples of things that may not work correctly on
     * Range operations without this iterator are
     * Edge/Entity/Undirected/Directed Edge filtering
     * This method May return null if this type of iterator is not required for example
     * if all needed filtering is applied elsewhere.
     *
     * @param operation the operation to get the IteratorSetting for
     * @return A new {@link IteratorSetting} for an Iterator capable of
     * filtering {@link uk.gov.gchq.gaffer.data.element.Element}s based on the
     * options defined in the gaffer.accumulostore.operation
     */
    IteratorSetting getElementPropertyRangeQueryFilter(final GetElementsOperation<?, ?> operation);

    /**
     * Returns the iterator settings for a given iterator name. Allowed iterator
     * names are: Aggregator, Validator and Bloom_Filter.
     *
     * @param store        the accumulo store
     * @param iteratorName the name of the iterator to return the settings for
     * @return the iterator settings for a given iterator name.
     * @throws IteratorSettingException if an iterator setting could not be created
     */
    IteratorSetting getIteratorSetting(final AccumuloStore store, final String iteratorName) throws IteratorSettingException;
}
