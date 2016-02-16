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

package gaffer.accumulostore.integration.operation.handler;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.MockAccumuloStore;
import gaffer.accumulostore.MockAccumuloStoreForTest;
import gaffer.accumulostore.operation.handler.GetAdjacentEntitySeedsHandler;
import gaffer.accumulostore.utils.Constants;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.PathUtil;
import gaffer.data.element.Element;
import gaffer.operation.data.EntitySeed;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.GetOperation;
import gaffer.operation.handler.AbstractGetAdjacentEntitySeedsHandlerTest;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.store.Store;
import gaffer.store.StoreException;
import gaffer.store.operation.handler.OperationHandler;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.BDDMockito.given;

public class GetAdjacentEntitySeedsHandlerIT extends AbstractGetAdjacentEntitySeedsHandlerTest {
    @Override
    protected MockAccumuloStore createMockStore() {
        return new MockAccumuloStoreForTest();
    }

    @Override
    protected String getEdgeGroup() {
        return TestGroups.EDGE;
    }

    @Override
    protected void addEdges(final List<Element> edges, final Store mockStore) {
        try {
            ((AccumuloStore) mockStore).addElements(edges);
        } catch (StoreException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected OperationHandler<GetAdjacentEntitySeeds, Iterable<EntitySeed>> createHandler() {
        return new GetAdjacentEntitySeedsHandler();
    }

    @Override
    protected View createView() {
        return View.fromJson(PathUtil.view(getClass()));
    }

    @Override
    protected GetAdjacentEntitySeeds createMockOperation(final GetOperation.IncludeIncomingOutgoingType inOutType) throws IOException {
        final GetAdjacentEntitySeeds operation = super.createMockOperation(inOutType);

        final Map<String, String> options = new HashMap<>();
        options.put(Constants.OPERATION_AUTHORISATIONS, "authorisation");
        options.put(Constants.OPERATION_MATCH_AS_SOURCE, "true");
        given(operation.getOptions()).willReturn(options);

        return operation;
    }
}
