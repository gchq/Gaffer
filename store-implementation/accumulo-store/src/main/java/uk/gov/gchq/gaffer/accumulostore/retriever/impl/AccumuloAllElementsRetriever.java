/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.retriever.impl;

import com.google.common.collect.Sets;
import org.apache.accumulo.core.data.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.accumulostore.key.exception.RangeFactoryException;
import uk.gov.gchq.gaffer.accumulostore.retriever.AccumuloItemRetriever;
import uk.gov.gchq.gaffer.accumulostore.retriever.RetrieverException;
import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyCloseableIterator;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;

import java.util.Iterator;
import java.util.Set;

/**
 * This allows queries for all elements.
 */
public class AccumuloAllElementsRetriever extends AccumuloItemRetriever {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloAllElementsRetriever.class);

    public AccumuloAllElementsRetriever(final AccumuloStore store, final Operation operation, final View view, final DirectedType directedType, final User user)
            throws IteratorSettingException, StoreException {
        super(store, operation, view, user, false,
                store.getKeyPackage().getIteratorFactory().getElementPropertyRangeQueryFilter(operation, view, directedType),
                store.getKeyPackage().getIteratorFactory().getElementPreAggregationFilterIteratorSetting(view, store),
                store.getKeyPackage().getIteratorFactory().getElementPostAggregationFilterIteratorSetting(view, store),
                store.getKeyPackage().getIteratorFactory().getEdgeEntityDirectionFilterIteratorSetting(operation, view, directedType),
                store.getKeyPackage().getIteratorFactory().getQueryTimeAggregatorIteratorSetting(view, store));
    }

    /**
     * Only 1 iterator can be open at a time.
     *
     * @return a closeable iterator of items.
     */
    @Override
    public Iterator<Element> iterator() {
        CloseableUtil.close(iterator);

        try {
            // A seed must be entered so the below add to ranges is reached.
            final Set<EntitySeed> all = Sets.newHashSet(new EntitySeed());
            iterator = new ElementIterator(all.iterator());
        } catch (final RetrieverException e) {
            LOGGER.error("{} returning empty iterator", e.getMessage(), e);
            return new EmptyCloseableIterator<>();
        }

        return iterator;
    }

    @Override
    protected void addToRanges(final Object seed, final Set<Range> ranges) throws RangeFactoryException {
        ranges.add(new Range());
    }
}
