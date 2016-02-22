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

package gaffer.accumulostore.retriever.impl;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.key.exception.IteratorSettingException;
import gaffer.accumulostore.key.exception.RangeFactoryException;
import gaffer.accumulostore.retriever.AccumuloItemRetriever;
import gaffer.operation.GetOperation;
import gaffer.operation.data.ElementSeed;
import gaffer.store.StoreException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import java.util.Set;

/**
 * This allows queries for all data related to the provided
 * {@link gaffer.operation.data.ElementSeed}s.
 */
public class AccumuloSingleIDRetriever
        extends AccumuloItemRetriever<GetOperation<? extends ElementSeed, ?>, ElementSeed> {

    public AccumuloSingleIDRetriever(final AccumuloStore store, final GetOperation<? extends ElementSeed, ?> operation)
            throws IteratorSettingException, StoreException {
        this(store, operation,
                store.getKeyPackage().getIteratorFactory().getElementFilterIteratorSetting(operation.getView(), store),
                store.getKeyPackage().getIteratorFactory().getEdgeEntityDirectionFilterIteratorSetting(operation));
    }

    /**
     * Use of the varargs parameter here will mean the usual default iterators
     * wont be applied, (Edge Direction,Edge/Entity Type and View Filtering) To
     * apply them pass them directly to the varargs via calling your
     * keyPackage.getIteratorFactory() and either
     * getElementFilterIteratorSetting and/Or
     * getEdgeEntityDirectionFilterIteratorSetting
     *
     * @param store            the accumulo store
     * @param operation        the get operation
     * @param iteratorSettings the iterator settings
     * @throws StoreException if any store issues occur
     */
    public AccumuloSingleIDRetriever(final AccumuloStore store, final GetOperation<? extends ElementSeed, ?> operation,
                                     final IteratorSetting... iteratorSettings) throws StoreException {
        super(store, operation, iteratorSettings);
    }

    @Override
    protected void addToRanges(final ElementSeed seed, final Set<Range> ranges) throws RangeFactoryException {
        ranges.addAll(rangeFactory.getRange(seed, operation));
    }
}
