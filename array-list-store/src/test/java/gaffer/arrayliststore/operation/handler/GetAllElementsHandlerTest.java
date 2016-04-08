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

package gaffer.arrayliststore.operation.handler;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import gaffer.arrayliststore.ArrayListStore;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.operation.handler.AbstractGetAllElementsHandlerTest;
import gaffer.operation.impl.get.GetAllElements;
import gaffer.store.Store;
import gaffer.store.operation.handler.OperationHandler;
import java.util.ArrayList;
import java.util.Collection;

public class GetAllElementsHandlerTest extends AbstractGetAllElementsHandlerTest {

    @Override
    protected ArrayListStore createMockStore() {
        return mock(ArrayListStore.class);
    }

    @Override
    protected void addEdges(final Collection<Edge> edges, final Store mockStore) {
        given((((ArrayListStore) mockStore)).getEdges()).willReturn(new ArrayList<>(edges));
    }

    @Override
    protected void addEntities(final Collection<Entity> entities, final Store mockStore) {
        given((((ArrayListStore) mockStore)).getEntities()).willReturn(new ArrayList<>(entities));
    }

    @Override
    protected OperationHandler<GetAllElements<Element>, Iterable<Element>> createGetAllElementsHandler() {
        return new GetAllElementsHandler();
    }
}