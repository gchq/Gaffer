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

import com.google.common.collect.Lists;
import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.key.IteratorSettingFactory;
import gaffer.accumulostore.key.exception.IteratorSettingException;
import gaffer.accumulostore.retriever.impl.AccumuloAllElementsRetriever;
import gaffer.data.element.Element;
import gaffer.operation.OperationException;
import gaffer.operation.impl.get.GetAllElements;
import gaffer.store.StoreException;
import gaffer.user.User;
import org.apache.accumulo.core.client.IteratorSetting;
import java.util.List;

public class GetAllElementsHandler extends AccumuloGetIterableHandler<GetAllElements<Element>, Element> {
    @Override
    protected Iterable<Element> doOperation(final GetAllElements<Element> operation,
                                            final User user, final AccumuloStore store) throws OperationException {
        final Iterable<Element> results;
        try {
            final IteratorSettingFactory iteratorFactory = store.getKeyPackage().getIteratorFactory();
            final List<IteratorSetting> iteratorSettings = Lists.newArrayList(
                    iteratorFactory.getElementPropertyRangeQueryFilter(operation),
                    iteratorFactory.getElementFilterIteratorSetting(operation.getView(), store),
                    iteratorFactory.getEdgeEntityDirectionFilterIteratorSetting(operation)
            );
            if (operation.isSummarise()) {
                iteratorSettings.add(iteratorFactory.getQueryTimeAggregatorIteratorSetting(store));
            }
            results = new AccumuloAllElementsRetriever(store, operation, user,
                    (IteratorSetting[]) iteratorSettings.toArray(new IteratorSetting[iteratorSettings.size()]));
        } catch (IteratorSettingException | StoreException e) {
            throw new OperationException("Failed to get elements", e);
        }

        return results;
    }
}
