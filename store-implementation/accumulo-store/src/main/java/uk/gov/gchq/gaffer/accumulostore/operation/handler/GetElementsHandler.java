/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.operation.handler;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.accumulostore.retriever.impl.AccumuloElementsRetriever;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloOperationHandlerUtils.BuilderInputInOutTypeViewDirectedType;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.FieldDeclaration;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import static java.util.Objects.nonNull;

import static uk.gov.gchq.gaffer.accumulostore.utils.AccumuloOperationHandlerUtils.DIRECTED_TYPE;
import static uk.gov.gchq.gaffer.accumulostore.utils.AccumuloOperationHandlerUtils.VIEW;
import static uk.gov.gchq.gaffer.accumulostore.utils.AccumuloOperationHandlerUtils.getInputInOutTypeViewDirectedTypeFieldDeclaration;

public class GetElementsHandler implements OperationHandler<Iterable<? extends Element>> {

    private static final String ACCUMULOSTORE_OPERATION_RETURN_MATCHED_ID_AS_EDGE_SOURCE = "accumulostore.operation.return_matched_id_as_edge_source";

    @Override
    public Iterable<? extends Element> _doOperation(final Operation operation, final Context context, final Store store) throws OperationException {
        try {
            // TODO: check
            if (nonNull(context.getVariable(ACCUMULOSTORE_OPERATION_RETURN_MATCHED_ID_AS_EDGE_SOURCE))) {
                throw new IllegalArgumentException("The accumulostore.operation.return_matched_id_as_edge_source option has been removed. "
                        + "Instead of flipping the Edges around the result Edges will have a matchedVertex field set specifying if the "
                        + "SOURCE or DESTINATION was matched.");
            } else {
                final AccumuloStore accumuloStore = AccumuloStore.class.cast(store);
                final View view = View.class.cast(operation.get(VIEW));
                final DirectedType directedType = DirectedType.class.cast(operation.get(DIRECTED_TYPE));

                return new AccumuloElementsRetriever(accumuloStore, operation, view, directedType, context.getUser());
            }
        } catch (final IteratorSettingException | StoreException e) {
            throw new OperationException("Failed to get elements", e);
        }
    }

    @Override
    public FieldDeclaration getFieldDeclaration() {
        return getInputInOutTypeViewDirectedTypeFieldDeclaration();
    }

    public static class OperationBuilder extends BuilderInputInOutTypeViewDirectedType<OperationBuilder, GetElementsHandler, Iterable<? extends ElementId>> {

        @Override
        protected OperationBuilder getBuilder() {
            return this;
        }

        @Override
        protected GetElementsHandler getHandler() {
            return new GetElementsHandler();
        }
    }
}
