/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.operation.handler;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.IteratorSettingFactory;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsBetweenSets;
import uk.gov.gchq.gaffer.accumulostore.retriever.impl.AccumuloIDBetweenSetsRetriever;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.user.User;

@Deprecated
public class GetElementsBetweenSetsHandler implements OutputOperationHandler<GetElementsBetweenSets, Iterable<? extends Element>> {

    @Override
    public Iterable<? extends Element> doOperation(final GetElementsBetweenSets operation,
                                                   final Context context, final Store store)
            throws OperationException {
        return doOperation(operation, context.getUser(), (AccumuloStore) store);
    }

    public Iterable<? extends Element> doOperation(final GetElementsBetweenSets operation,
                                                   final User user, final AccumuloStore store)
            throws OperationException {
        try {
            final IteratorSettingFactory iteratorFactory = store.getKeyPackage().getIteratorFactory();
            return new AccumuloIDBetweenSetsRetriever(store, operation, user,
                    iteratorFactory.getElementPreAggregationFilterIteratorSetting(operation.getView(), store),
                    iteratorFactory.getElementPostAggregationFilterIteratorSetting(operation.getView(), store),
                    iteratorFactory.getEdgeEntityDirectionFilterIteratorSetting(operation),
                    iteratorFactory.getQueryTimeAggregatorIteratorSetting(operation.getView(), store));
        } catch (final IteratorSettingException | StoreException e) {
            throw new OperationException("Failed to get elements", e);
        }
    }

}
