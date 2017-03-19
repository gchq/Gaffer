/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.key.core;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.hadoop.util.bloom.BloomFilter;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.IteratorSettingFactory;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.CoreKeyBloomFilterIterator;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.CoreKeyGroupByAggregatorIterator;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.accumulostore.key.impl.AggregatorIterator;
import uk.gov.gchq.gaffer.accumulostore.key.impl.ElementPostAggregationFilter;
import uk.gov.gchq.gaffer.accumulostore.key.impl.ElementPreAggregationFilter;
import uk.gov.gchq.gaffer.accumulostore.key.impl.RowIDAggregator;
import uk.gov.gchq.gaffer.accumulostore.key.impl.ValidatorFilter;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.IteratorSettingBuilder;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;

public abstract class AbstractCoreKeyIteratorSettingsFactory implements IteratorSettingFactory {

    @Override
    public IteratorSetting getBloomFilterIteratorSetting(final BloomFilter filter) throws IteratorSettingException {
        return new IteratorSettingBuilder(AccumuloStoreConstants.BLOOM_FILTER_ITERATOR_PRIORITY,
                AccumuloStoreConstants.BLOOM_FILTER_ITERATOR_NAME, CoreKeyBloomFilterIterator.class).bloomFilter(filter).build();
    }

    @Override
    public IteratorSetting getElementPreAggregationFilterIteratorSetting(final View view, final AccumuloStore store)
            throws IteratorSettingException {
        return new IteratorSettingBuilder(AccumuloStoreConstants.ELEMENT_PRE_AGGREGATION_FILTER_ITERATOR_PRIORITY,
                AccumuloStoreConstants.ELEMENT_PRE_AGGREGATION_FILTER_ITERATOR_NAME, ElementPreAggregationFilter.class).schema(store.getSchema())
                .view(view).keyConverter(store.getKeyPackage().getKeyConverter()).build();
    }

    @Override
    public IteratorSetting getElementPostAggregationFilterIteratorSetting(final View view, final AccumuloStore store)
            throws IteratorSettingException {
        return new IteratorSettingBuilder(AccumuloStoreConstants.ELEMENT_POST_AGGREGATION_FILTER_ITERATOR_PRIORITY,
                AccumuloStoreConstants.ELEMENT_POST_AGGREGATION_FILTER_ITERATOR_NAME, ElementPostAggregationFilter.class).schema(store.getSchema())
                .view(view).keyConverter(store.getKeyPackage().getKeyConverter()).build();
    }

    @Override
    public IteratorSetting getAggregatorIteratorSetting(final AccumuloStore store) throws IteratorSettingException {
        return new IteratorSettingBuilder(AccumuloStoreConstants.AGGREGATOR_ITERATOR_PRIORITY,
                AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME, AggregatorIterator.class)
                .all()
                .schema(store.getSchema())
                .keyConverter(store.getKeyPackage().getKeyConverter())
                .build();
    }

    @Override
    public IteratorSetting getRowIDAggregatorIteratorSetting(final AccumuloStore store, final String columnFamily) throws IteratorSettingException {
        if (!store.getSchema().hasAggregators()) {
            return null;
        }

        return new IteratorSettingBuilder(AccumuloStoreConstants.ROW_ID_AGGREGATOR_ITERATOR_PRIORITY,
                AccumuloStoreConstants.ROW_ID_AGGREGATOR_ITERATOR_NAME, RowIDAggregator.class)
                .all()
                .columnFamily(columnFamily)
                .schema(store.getSchema())
                .keyConverter(store.getKeyPackage().getKeyConverter())
                .build();
    }

    @Override
    public IteratorSetting getValidatorIteratorSetting(final AccumuloStore store) {
        return new IteratorSettingBuilder(AccumuloStoreConstants.VALIDATOR_ITERATOR_PRIORITY,
                AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME, ValidatorFilter.class)
                .all()
                .schema(store.getSchema())
                .keyConverter(store.getKeyPackage().getKeyConverter())
                .build();
    }

    @Override
    public IteratorSetting getQueryTimeAggregatorIteratorSetting(final View view, final AccumuloStore store)
            throws IteratorSettingException {
        if (!queryTimeAggregatorRequired(view, store)) {
            return null;
        }
        return new IteratorSettingBuilder(AccumuloStoreConstants.COLUMN_QUALIFIER_AGGREGATOR_ITERATOR_PRIORITY,
                AccumuloStoreConstants.COLUMN_QUALIFIER_AGGREGATOR_ITERATOR_NAME, CoreKeyGroupByAggregatorIterator.class)
                .all()
                .schema(store.getSchema())
                .view(view)
                .keyConverter(store.getKeyPackage().getKeyConverter())
                .build();
    }

    public boolean queryTimeAggregatorRequired(final View view, final AccumuloStore store) {
        Schema schema = store.getSchema();
        if (!schema.hasAggregators()) {
            return false;
        }

        String visibilityProp = schema.getVisibilityProperty();
        for (final String edgeGroup : view.getEdgeGroups()) {
            SchemaEdgeDefinition edgeDefinition = schema.getEdge(edgeGroup);
            if (edgeDefinition.containsProperty(visibilityProp)) {
                return true;
            }
            ViewElementDefinition viewElementDefinition = view.getEdge(edgeGroup);
            if (viewElementDefinition.getGroupBy() != null) {
                if (edgeDefinition.getGroupBy().size() != viewElementDefinition.getGroupBy().size()) {
                    return true;
                }
            }
        }
        for (final String entityGroup : view.getEntityGroups()) {
            SchemaEntityDefinition entityDefinition = schema.getEntity(entityGroup);
            if (entityDefinition.containsProperty(visibilityProp)) {
                return true;
            }
            ViewElementDefinition viewElementDefinition = view.getElement(entityGroup);
            if (viewElementDefinition.getGroupBy() != null) {
                if (entityDefinition.getGroupBy().size() != viewElementDefinition.getGroupBy().size()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public IteratorSetting getIteratorSetting(final AccumuloStore store, final String iteratorName) throws IteratorSettingException {
        switch (iteratorName) {
            case AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME:
                return getAggregatorIteratorSetting(store);
            case AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME:
                return getValidatorIteratorSetting(store);
            default:
                throw new IllegalArgumentException("Iterator name is not allowed: " + iteratorName
                        + ". Allowed iterator names are: "
                        + AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME + ","
                        + AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME);
        }
    }
}
