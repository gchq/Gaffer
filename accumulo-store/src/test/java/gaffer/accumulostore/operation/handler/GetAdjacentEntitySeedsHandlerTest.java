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

import static org.mockito.BDDMockito.given;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.MockAccumuloStore;
import gaffer.accumulostore.MockAccumuloStoreForTest;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.commonutil.StreamUtil;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.GetOperation;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.handler.AbstractGetAdjacentEntitySeedsHandlerTest;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.store.Store;
import gaffer.store.StoreException;
import gaffer.store.operation.handler.OperationHandler;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetAdjacentEntitySeedsHandlerTest extends AbstractGetAdjacentEntitySeedsHandlerTest {
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
        return View.fromJson(StreamUtil.view(getClass()));
    }

    @Override
    protected GetAdjacentEntitySeeds createMockOperation(final GetOperation.IncludeIncomingOutgoingType inOutType) throws IOException {
        final GetAdjacentEntitySeeds operation = super.createMockOperation(inOutType);

        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.OPERATION_RETURN_MATCHED_SEEDS_AS_EDGE_SOURCE, "true");
        given(operation.getOptions()).willReturn(options);

        return operation;
    }
}
