/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.hbasestore.operation.handler;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.TransformIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.IsEdgeValidator;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.retriever.HBaseRetriever;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collections;

public class GetAdjacentIdsHandler implements OutputOperationHandler<GetAdjacentIds, CloseableIterable<? extends EntityId>> {

    @Override
    public CloseableIterable<? extends EntityId> doOperation(final GetAdjacentIds operation,
                                                             final Context context, final Store store)
            throws OperationException {
        return doOperation(operation, context.getUser(), (HBaseStore) store);
    }

    private CloseableIterable<? extends EntityId> doOperation(final GetAdjacentIds op,
                                                              final User user,
                                                              final HBaseStore store)
            throws OperationException {
        if (null == op.getInput()) {
            // If null seeds no results are returned
            return new WrappedCloseableIterable<>();
        }

        final HBaseRetriever<?> edgeRetriever;
        final GetElements getEdges = new GetElements.Builder()
                .options(op.getOptions())
                .view(new View.Builder()
                        .merge(op.getView())
                        .entities(Collections.emptyMap())
                        .build())
                .inputIds(op.getInput())
                .directedType(op.getDirectedType())
                .inOutType(op.getIncludeIncomingOutGoing())
                .build();

        try {
            edgeRetriever = store.createRetriever(getEdges, user, getEdges.getInput(), true);
        } catch (final StoreException e) {
            throw new OperationException(e.getMessage(), e);
        }

        return new ExtractDestinationEntityId(edgeRetriever);
    }

    protected static final class ExtractDestinationEntityId extends TransformIterable<Element, EntityId> {
        private ExtractDestinationEntityId(final Iterable<Element> input) {
            super(input, new IsEdgeValidator());
        }

        @Override
        protected EntityId transform(final Element element) {
            final EntitySeed nextId;
            if (EdgeId.MatchedVertex.DESTINATION == ((EdgeId) element).getMatchedVertex()) {
                nextId = new EntitySeed(((EdgeId) element).getSource());
            } else {
                nextId = new EntitySeed(((EdgeId) element).getDestination());
            }

            return nextId;
        }

        @Override
        public void close() {
            ((CloseableIterable) super.getInput()).close();
        }

        @Override
        protected Iterable<? extends Element> getInput() {
            return super.getInput();
        }
    }
}
