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

import gaffer.arrayliststore.ArrayListStore;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Element;
import gaffer.operation.data.EntitySeed;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.handler.AbstractGetAdjacentEntitySeedsHandlerTest;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.store.Store;
import gaffer.store.operation.handler.OperationHandler;
import java.util.List;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class GetAdjacentEntitySeedsHandlerTest extends AbstractGetAdjacentEntitySeedsHandlerTest {

    @Override
    protected ArrayListStore createMockStore() {
        return mock(ArrayListStore.class);
    }

    @Override
    protected String getEdgeGroup() {
        return TestGroups.EDGE;
    }

    @Override
    protected void addEdges(final List<Element> edges, final Store mockStore) {
        given((((ArrayListStore) mockStore)).getEdges()).willReturn((List) edges);
    }

    @Override
    protected OperationHandler<GetAdjacentEntitySeeds, Iterable<EntitySeed>> createHandler() {
        return new GetAdjacentEntitySeedsHandler();
    }

    @Override
    protected View createView() {
        return mock(View.class);
    }
}