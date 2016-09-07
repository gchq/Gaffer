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

package gaffer.accumulostore.operation.handler;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.key.IteratorSettingFactory;
import gaffer.accumulostore.key.exception.IteratorSettingException;
import gaffer.accumulostore.retriever.impl.AccumuloAllElementsRetriever;
import gaffer.data.element.Element;
import gaffer.operation.OperationException;
import gaffer.operation.impl.get.GetAllElements;
import gaffer.store.Context;
import gaffer.store.Store;
import gaffer.store.StoreException;
import gaffer.store.operation.handler.OperationHandler;
import gaffer.user.User;

public class GetAllElementsHandler implements OperationHandler<GetAllElements<Element>, Iterable<Element>> {
    @Override
    public Iterable<Element> doOperation(final GetAllElements<Element> operation, final Context context, final Store store)
            throws OperationException {
        return doOperation(operation, context.getUser(), (AccumuloStore) store);
    }

    public Iterable<Element> doOperation(final GetAllElements<Element> operation, final User user, final AccumuloStore store) throws OperationException {
        final IteratorSettingFactory iteratorFactory = store.getKeyPackage().getIteratorFactory();
        try {
            return new AccumuloAllElementsRetriever(store, operation, user, iteratorFactory.getElementPropertyRangeQueryFilter(operation),
                    iteratorFactory.getElementPreAggregationFilterIteratorSetting(operation.getView(), store),
                    iteratorFactory.getElementPostAggregationFilterIteratorSetting(operation.getView(), store),
                    iteratorFactory.getEdgeEntityDirectionFilterIteratorSetting(operation),
                    iteratorFactory.getQueryTimeAggregatorIteratorSetting(operation.getView(), store));
        } catch (IteratorSettingException | StoreException e) {
            throw new OperationException("Failed to get elements", e);
        }
    }
}
