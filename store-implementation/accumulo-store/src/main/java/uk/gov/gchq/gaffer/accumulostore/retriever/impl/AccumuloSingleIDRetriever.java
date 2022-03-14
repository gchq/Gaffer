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

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.accumulostore.key.exception.RangeFactoryException;
import uk.gov.gchq.gaffer.accumulostore.retriever.AccumuloItemRetriever;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;

import java.util.Set;

/**
 * This allows queries for all data related to the provided {@link ElementId}s.
 */
public class AccumuloSingleIDRetriever extends AccumuloItemRetriever {

    private final Operation operation;

    public AccumuloSingleIDRetriever(final AccumuloStore store, final Operation operation,
                                     final View view, final DirectedType directedType, final User user)
            throws IteratorSettingException, StoreException {
        this(store, operation, view, user,
                store.getKeyPackage().getIteratorFactory().getElementPreAggregationFilterIteratorSetting(view, store),
                store.getKeyPackage().getIteratorFactory().getElementPostAggregationFilterIteratorSetting(view, store),
                store.getKeyPackage().getIteratorFactory().getEdgeEntityDirectionFilterIteratorSetting(operation, view, directedType));
    }

    public AccumuloSingleIDRetriever(final AccumuloStore store, final Operation operation,
                                     final View view, final User user,
                                     final IteratorSetting... iteratorSettings)
            throws StoreException {
        this(store, operation, view, user, true, iteratorSettings);
    }

    /**
     * Use of the varargs parameter here will mean the usual default iterators
     * wont be applied, (Edge Direction,Edge/Entity TypeDefinition and View Filtering) To
     * apply them pass them directly to the varargs via calling your
     * keyPackage.getIteratorFactory() and either
     * getElementFilterIteratorSetting and/Or
     * getEdgeEntityDirectionFilterIteratorSetting
     *
     * @param store the accumulo store
     * @param operation the get operation
     * @param view the user view
     * @param user the user executing the operation
     * @param includeMatchedVertex true if the matched vertex field should be set
     * @param iteratorSettings the iterator settings
     * @throws StoreException if any store issues occur
     */
    public AccumuloSingleIDRetriever(final AccumuloStore store, final Operation operation,
                                     final View view, final User user,
                                     final boolean includeMatchedVertex,
                                     final IteratorSetting... iteratorSettings)
            throws StoreException {
        super(store, operation, view, user, includeMatchedVertex, iteratorSettings);
        this.operation = operation;
    }

    @Override
    protected void addToRanges(final Object seed, final Set<Range> ranges) throws RangeFactoryException {
        ranges.addAll(rangeFactory.getRange(ElementId.class.cast(seed), operation));
    }
}
