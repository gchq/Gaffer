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
import gaffer.accumulostore.key.exception.IteratorSettingException;
import gaffer.accumulostore.operation.impl.GetElementsBetweenSets;
import gaffer.accumulostore.retriever.impl.AccumuloIDBetweenSetsRetriever;
import gaffer.data.element.Element;
import gaffer.operation.OperationException;
import gaffer.store.StoreException;
import gaffer.user.User;

public class GetElementsBetweenSetsHandler extends AccumuloGetIterableHandler<GetElementsBetweenSets<Element>, Element> {

    @Override
    protected Iterable<Element> doOperation(final GetElementsBetweenSets<Element> operation,
                                            final User user, final AccumuloStore store)
            throws OperationException {
        final Iterable<Element> results;
        try {
            if (operation.isSummarise()) {
                results = new AccumuloIDBetweenSetsRetriever(store, operation, user,
                        store.getKeyPackage().getIteratorFactory().getQueryTimeAggregatorIteratorSetting(store));
            } else {
                results = new AccumuloIDBetweenSetsRetriever(store, operation, user);
            }
        } catch (IteratorSettingException | StoreException e) {
            throw new OperationException("Failed to get elements", e);
        }

        return results;
    }

}
