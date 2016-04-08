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
import gaffer.accumulostore.MockAccumuloStore;
import gaffer.accumulostore.MockAccumuloStoreForTest;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.operation.handler.AbstractGetAllElementsHandlerTest;
import gaffer.operation.impl.get.GetAllElements;
import gaffer.store.Store;
import gaffer.store.StoreException;
import gaffer.store.operation.handler.OperationHandler;
import java.util.Collection;

public class GetAllElementsHandlerTest extends AbstractGetAllElementsHandlerTest {
    @Override
    protected MockAccumuloStore createMockStore() {
        return new MockAccumuloStoreForTest();
    }

    @Override
    protected void addEdges(final Collection<Edge> edges, final Store mockStore) {
        try {
            ((AccumuloStore) mockStore).addElements((Collection) edges);
        } catch (StoreException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void addEntities(final Collection<Entity> entities, final Store mockStore) {
        try {
            ((AccumuloStore) mockStore).addElements((Collection) entities);
        } catch (StoreException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected OperationHandler<GetAllElements<Element>, Iterable<Element>> createGetAllElementsHandler() {
        return new GetAllElementsHandler();
    }
}