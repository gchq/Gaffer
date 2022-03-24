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
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.ValidatedElements;
import uk.gov.gchq.gaffer.store.operation.handler.FieldDeclaration;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

public class AddElementsHandler implements OperationHandler<Void> {

    private static final String SKIP_INVALID_ELEMENTS = "skipInvalidElements";

    private static final String VALIDATE = "validate";

    @Override
    public Void _doOperation(final Operation operation, final Context context, final Store store) throws OperationException {
        try {
            final boolean validate = Boolean.class.cast(operation.getOrDefault(VALIDATE, true));
            final boolean skipInvalidElements = Boolean.class.cast(operation.getOrDefault(SKIP_INVALID_ELEMENTS, false));
            final AccumuloStore accumuloStore = AccumuloStore.class.cast(store);

            @SuppressWarnings("unchecked")
            Iterable<? extends Element> elements = (Iterable<? extends Element>) operation.input();
            if (validate) {
                elements = new ValidatedElements(elements, accumuloStore.getSchema(), skipInvalidElements);
            }

            accumuloStore.addElements(elements);

            return null;
        } catch (final StoreException e) {
            throw new OperationException("Failed to add elements", e);
        }
    }

    @Override
    public FieldDeclaration getFieldDeclaration() {
        return new FieldDeclaration()
                .inputRequired(Iterable.class)
                .fieldOptional(VALIDATE, Boolean.class)
                .fieldOptional(SKIP_INVALID_ELEMENTS, Boolean.class);
    }

    public static class OperationBuilder extends BuilderSpecificInputOperation<OperationBuilder, AddElementsHandler> {

        public OperationBuilder validate(final Boolean validate) {
            operation.operationArg(VALIDATE, validate);
            return this;
        }

        public OperationBuilder skipInvalidElements(final Boolean skipInvalidElements) {
            operation.operationArg(SKIP_INVALID_ELEMENTS, skipInvalidElements);
            return this;
        }

        @Override
        protected OperationBuilder getBuilder() {
            return this;
        }

        @Override
        protected AddElementsHandler getHandler() {
            return new AddElementsHandler();
        }
    }
}
