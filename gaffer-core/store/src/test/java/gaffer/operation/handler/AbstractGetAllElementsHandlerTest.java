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

package gaffer.operation.handler;

import gaffer.data.element.Element;
import gaffer.operation.GetOperation.IncludeEdgeType;
import gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import gaffer.operation.GetOperation.SeedMatchingType;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.impl.get.GetAllElements;
import gaffer.operation.impl.get.GetElements;
import gaffer.store.operation.handler.OperationHandler;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractGetAllElementsHandlerTest extends AbstractGetElementsHandlerTest {
    @Test
    @Override
    public void shouldGetElements() throws Exception {
        for (boolean includeEntities : Arrays.asList(true, false)) {
            for (IncludeEdgeType includeEdgeType : IncludeEdgeType.values()) {
                if (!includeEntities && IncludeEdgeType.NONE == includeEdgeType) {
                    // Cannot query for nothing!
                    continue;
                }
                try {
                    shouldGetAllElements(includeEntities, includeEdgeType);
                } catch (AssertionError e) {
                    throw new AssertionError("GetElementsBySeed failed with parameters: includeEntities=" + includeEntities
                            + ", includeEdgeType=" + includeEdgeType.name(), e);
                }
            }
        }
    }

    protected void shouldGetAllElements(boolean includeEntities, final IncludeEdgeType includeEdgeType) throws Exception {
        final List<ElementSeed> expectedSeeds = new ArrayList<>();
        if (includeEntities) {
            expectedSeeds.addAll(getEntities().keySet());
        }

        if (IncludeEdgeType.ALL == includeEdgeType || IncludeEdgeType.DIRECTED == includeEdgeType) {
            expectedSeeds.addAll(getDirEdges().keySet());
        }

        if (IncludeEdgeType.ALL == includeEdgeType || IncludeEdgeType.UNDIRECTED == includeEdgeType) {
            expectedSeeds.addAll(getUndirEdges().keySet());
        }

        shouldGetElements(expectedSeeds, SeedMatchingType.EQUAL, includeEdgeType, includeEntities, IncludeIncomingOutgoingType.OUTGOING, null);
    }

    @Override
    protected GetElements<ElementSeed, Element> createMockOperation(final SeedMatchingType seedMatching,
                                                                    final IncludeEdgeType includeEdgeType,
                                                                    final Boolean includeEntities,
                                                                    final IncludeIncomingOutgoingType inOutType,
                                                                    final Iterable<ElementSeed> seeds) throws IOException {
        final GetAllElements<Element> operation = new GetAllElements.Builder<>()
                .includeEntities(includeEntities)
                .includeEdges(includeEdgeType)
                .populateProperties(true)
                .view(createView())
                .build();

        operation.setIncludeIncomingOutGoing(inOutType);
        return operation;
    }

    @Override
    protected OperationHandler<GetElements<ElementSeed, Element>, Iterable<Element>> createHandler() {
        return (OperationHandler) createGetAllElementsHandler();
    }

    protected abstract OperationHandler<GetAllElements<Element>, Iterable<Element>> createGetAllElementsHandler();
}