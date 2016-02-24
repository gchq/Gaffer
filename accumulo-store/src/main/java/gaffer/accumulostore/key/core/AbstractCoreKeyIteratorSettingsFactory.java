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

package gaffer.accumulostore.key.core;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.key.IteratorSettingFactory;
import gaffer.accumulostore.key.core.impl.CoreKeyBloomFilterIterator;
import gaffer.accumulostore.key.core.impl.CoreKeyColumnQualifierVisibilityValueAggregatorIterator;
import gaffer.accumulostore.key.exception.IteratorSettingException;
import gaffer.accumulostore.key.impl.AggregatorIterator;
import gaffer.accumulostore.key.impl.ElementFilter;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.accumulostore.utils.IteratorSettingBuilder;
import gaffer.data.elementdefinition.view.View;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.hadoop.util.bloom.BloomFilter;

public abstract class AbstractCoreKeyIteratorSettingsFactory implements IteratorSettingFactory {
    private static final String ELEMENT_FILTER_CLASS_NAME = ElementFilter.class.getName();

    @Override
    public IteratorSetting getBloomFilterIteratorSetting(final BloomFilter filter) throws IteratorSettingException {
        return new IteratorSettingBuilder(AccumuloStoreConstants.BLOOM_FILTER_ITERATOR_PRIORITY,
                AccumuloStoreConstants.BLOOM_FILTER_ITERATOR_NAME, CoreKeyBloomFilterIterator.class).bloomFilter(filter).build();
    }

    @Override
    public IteratorSetting getElementFilterIteratorSetting(final View view, final AccumuloStore store)
            throws IteratorSettingException {
        return new IteratorSettingBuilder(AccumuloStoreConstants.ELEMENT_FILTER_ITERATOR_PRIORITY,
                AccumuloStoreConstants.ELEMENT_FILTER_ITERATOR_NAME, ELEMENT_FILTER_CLASS_NAME).storeSchema(store.getStoreSchema())
                .view(view).keyConverter(store.getKeyPackage().getKeyConverter()).build();
    }

    /**
     * Returns an Iterator that will aggregate values in the accumulo table,
     * this iterator will be applied to the table on creation
     * <p>
     *
     * @param store the accumulo store
     * @return A new {@link IteratorSetting} for an Iterator that will aggregate
     * elements where they have the same key based on the
     * {@link gaffer.data.elementdefinition.schema.DataSchema}
     */
    @Override
    public IteratorSetting getAggregatorIteratorSetting(final AccumuloStore store) throws IteratorSettingException {
        return new IteratorSettingBuilder(AccumuloStoreConstants.AGGREGATOR_ITERATOR_PRIORITY, AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME,
                AggregatorIterator.class).all().dataSchema(store.getDataSchema()).storeSchema(store.getStoreSchema())
                .keyConverter(store.getKeyPackage().getKeyConverter()).build();
    }

    @Override
    public IteratorSetting getQueryTimeAggregatorIteratorSetting(final AccumuloStore store)
            throws IteratorSettingException {
        return new IteratorSettingBuilder(AccumuloStoreConstants.QUERY_TIME_AGGREGATOR_PRIORITY,
                AccumuloStoreConstants.QUERY_TIME_AGGREGATION_ITERATOR_NAME,
                CoreKeyColumnQualifierVisibilityValueAggregatorIterator.class).all().dataSchema(store.getDataSchema())
                .storeSchema(store.getStoreSchema()).keyConverter(store.getKeyPackage().getKeyConverter())
                .build();
    }

}
