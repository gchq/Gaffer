/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gaffer.accumulostore.operation.handler;

import gaffer.accumulostore.key.exception.IteratorSettingException;
import gaffer.accumulostore.utils.Constants;
import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.retriever.AccumuloRetriever;
import gaffer.accumulostore.retriever.impl.AccumuloSingleIDRetriever;
import gaffer.data.IsEdgeValidator;
import gaffer.data.TransformIterable;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.GetOperation.IncludeEdgeType;
import gaffer.operation.OperationException;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.store.Store;
import gaffer.store.StoreException;
import gaffer.store.operation.handler.OperationHandler;

public class GetAdjacentEntitySeedsHandler implements OperationHandler<GetAdjacentEntitySeeds, Iterable<EntitySeed>> {

    @Override
    public Iterable<EntitySeed> doOperation(final GetAdjacentEntitySeeds operation, final Store store) throws OperationException {
        return doOperation(operation, (AccumuloStore) store);
    }

    public Iterable<EntitySeed> doOperation(final GetAdjacentEntitySeeds operation, final AccumuloStore store) throws OperationException {
        operation.addOption(Constants.OPERATION_MATCH_AS_SOURCE, "true");

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

        return new TransformIterable<Element, EntitySeed>(edgeRetriever, new IsEdgeValidator()) {
            @Override
            protected EntitySeed transform(final Element element) {
                return new EntitySeed(((Edge) element).getDestination());
            }
        };
    }
}