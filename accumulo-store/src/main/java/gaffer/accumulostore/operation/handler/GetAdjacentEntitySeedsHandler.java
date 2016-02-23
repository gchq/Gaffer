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
import gaffer.accumulostore.retriever.AccumuloRetriever;
import gaffer.accumulostore.retriever.impl.AccumuloSingleIDRetriever;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.data.IsEdgeValidator;
import gaffer.data.TransformIterable;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.operation.GetOperation.IncludeEdgeType;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.store.Store;
import gaffer.store.StoreException;
import gaffer.store.operation.handler.OperationHandler;

public class GetAdjacentEntitySeedsHandler implements OperationHandler<GetAdjacentEntitySeeds, Iterable<EntitySeed>> {

    @Override
    public Iterable<EntitySeed> doOperation(final GetAdjacentEntitySeeds operation, final Store store)
            throws OperationException {
        return doOperation(operation, (AccumuloStore) store);
    }

    public Iterable<EntitySeed> doOperation(final GetAdjacentEntitySeeds operation, final AccumuloStore store)
            throws OperationException {
        operation.addOption(AccumuloStoreConstants.OPERATION_RETURN_MATCHED_SEEDS_AS_EDGE_SOURCE, "true");

        final AccumuloRetriever<?> edgeRetriever;
        try {
            operation.setIncludeEntities(false);
            if (IncludeEdgeType.NONE == operation.getIncludeEdges()) {
                operation.setIncludeEdges(IncludeEdgeType.ALL);
            }
            edgeRetriever = new AccumuloSingleIDRetriever(store, operation);
        } catch (IteratorSettingException | StoreException e) {
            throw new OperationException(e.getMessage(), e);
        }

        return new ExtractDestinationEntitySeed(edgeRetriever);
    }

    private static final class ExtractDestinationEntitySeed extends TransformIterable<Element, EntitySeed> {
        private ExtractDestinationEntitySeed(final Iterable<Element> input) {
            super(input, new IsEdgeValidator());
        }

        @Override
        protected EntitySeed transform(final Element element) {
            return new EntitySeed(((Edge) element).getDestination());
        }
    }
}
