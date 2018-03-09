/*
 * Copyright 2016-2018 Crown Copyright
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

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.accumulostore.key.exception.RangeFactoryException;
import uk.gov.gchq.gaffer.accumulostore.retriever.AccumuloItemRetriever;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;

import java.util.Set;

/**
 * This allows queries for all data related to the provided
 * {@link ElementId}s.
 */
public class AccumuloSingleIDRetriever<OP extends InputOutput<Iterable<? extends ElementId>, CloseableIterable<? extends Element>> & GraphFilters>
        extends AccumuloItemRetriever<OP, ElementId> {
    public AccumuloSingleIDRetriever(final AccumuloStore store, final OP operation,
                                     final User user)
            throws IteratorSettingException, StoreException {
        this(store, operation, user,
                store.getKeyPackage().getIteratorFactory().getElementPreAggregationFilterIteratorSetting(operation.getView(), store),
                store.getKeyPackage().getIteratorFactory().getElementPostAggregationFilterIteratorSetting(operation.getView(), store),
                store.getKeyPackage().getIteratorFactory().getEdgeEntityDirectionFilterIteratorSetting(operation));
    }

    public AccumuloSingleIDRetriever(final AccumuloStore store, final OP operation,
                                     final User user,
                                     final IteratorSetting... iteratorSettings) throws StoreException {
        this(store, operation, user, true, iteratorSettings);
    }

    /**
     * Use of the varargs parameter here will mean the usual default iterators
     * wont be applied, (Edge Direction,Edge/Entity TypeDefinition and View Filtering) To
     * apply them pass them directly to the varargs via calling your
     * keyPackage.getIteratorFactory() and either
     * getElementFilterIteratorSetting and/Or
     * getEdgeEntityDirectionFilterIteratorSetting
     *
     * @param store                the accumulo store
     * @param operation            the get operation
     * @param user                 the user executing the operation
     * @param includeMatchedVertex true if the matched vertex field should be set
     * @param iteratorSettings     the iterator settings
     * @throws StoreException if any store issues occur
     */
    public AccumuloSingleIDRetriever(final AccumuloStore store, final OP operation,
                                     final User user,
                                     final boolean includeMatchedVertex,
                                     final IteratorSetting... iteratorSettings) throws StoreException {
        super(store, operation, user, includeMatchedVertex, iteratorSettings);
    }

    @Override
    protected void addToRanges(final ElementId seed, final Set<Range> ranges) throws RangeFactoryException {
        ranges.addAll(rangeFactory.getRange(seed, operation));
    }
}
