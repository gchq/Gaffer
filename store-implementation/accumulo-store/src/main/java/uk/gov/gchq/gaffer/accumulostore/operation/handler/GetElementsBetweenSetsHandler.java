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
import uk.gov.gchq.gaffer.accumulostore.key.IteratorSettingFactory;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.accumulostore.retriever.impl.AccumuloIDBetweenSetsRetriever;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.FieldDeclaration;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.util.Arrays;

public class GetElementsBetweenSetsHandler implements OperationHandler<Iterable<? extends Element>> {

    private static final String INCLUDE_INCOMING_OUTGOING = "includeIncomingOutgoing";

    private static final String DIRECTED_TYPE = "directedType";

    @Override
    public Iterable<? extends Element> _doOperation(final Operation operation, final Context context, final Store store) throws OperationException {
        try {
            final AccumuloStore accumuloStore = (AccumuloStore) store;
            final View view = (View) operation.get("view");
            final DirectedType directedType = (DirectedType) operation.get(DIRECTED_TYPE);

            // TODO: pass in IteratorSettingFactory rather than multiple IteratorSettings?
            final IteratorSettingFactory iteratorFactory = accumuloStore.getKeyPackage().getIteratorFactory();
            return new AccumuloIDBetweenSetsRetriever(accumuloStore, operation, view, context.getUser(),
                    iteratorFactory.getElementPreAggregationFilterIteratorSetting(view, accumuloStore),
                    iteratorFactory.getElementPostAggregationFilterIteratorSetting(view, accumuloStore),
                    iteratorFactory.getEdgeEntityDirectionFilterIteratorSetting(operation, view, directedType),
                    iteratorFactory.getQueryTimeAggregatorIteratorSetting(view, accumuloStore));
        } catch (final IteratorSettingException | StoreException e) {
            throw new OperationException("Failed to get elements", e);
        }
    }

    @Override
    public FieldDeclaration getFieldDeclaration() {
        return new FieldDeclaration()
                .inputRequired(Iterable.class)
                .fieldOptional(DIRECTED_TYPE, DirectedType.class)
                .fieldOptional(INCLUDE_INCOMING_OUTGOING, IncludeIncomingOutgoingType.class);
    }

    static class OperationBuilder extends BuilderSpecificOperation<OperationBuilder, GetElementsBetweenSetsHandler> {

        public OperationBuilder input(final Iterable<? extends EntityId> inputA, final Iterable<? extends EntityId> inputB) {
            operation.operationArg(FieldDeclaration.INPUT, new Iterable[] {inputA, inputB});
            return this;
        }

        public OperationBuilder directedType(final DirectedType directedType) {
            operation.operationArg(DIRECTED_TYPE, directedType);
            return this;
        }

        public OperationBuilder includeIncomingOutgoing(final IncludeIncomingOutgoingType includeIncomingOutgoing) {
            operation.operationArg(INCLUDE_INCOMING_OUTGOING, includeIncomingOutgoing);
            return this;
        }

        @Override
        protected OperationBuilder getBuilder() {
            return this;
        }

        @Override
        protected GetElementsBetweenSetsHandler getHandler() {
            return new GetElementsBetweenSetsHandler();
        }
    }
}
