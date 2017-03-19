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

package uk.gov.gchq.gaffer.arrayliststore.operation.handler;

import uk.gov.gchq.gaffer.arrayliststore.ArrayListStore;
import uk.gov.gchq.gaffer.data.TransformIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;

public class AddElementsHandler implements OperationHandler<AddElements, Void> {
    @Override
    public Void doOperation(final AddElements operation,
                            final Context context, final Store store)
            throws OperationException {
        addElements(operation, (ArrayListStore) store);
        return null;
    }

    private void addElements(final AddElements operation, final ArrayListStore store) {
        store.addElements(new ElementCleaner(operation.getElements(), store));
    }

    private static final class ElementCleaner extends TransformIterable<Element, Element> {
        private final Store store;

        private ElementCleaner(final Iterable<Element> input, final Store store) {
            super(input);
            this.store = store;
        }

        @Override
        protected Element transform(final Element element) {
            final Element cleanElement = element.emptyClone();
            final SchemaElementDefinition elementDefinition = store.getSchema().getElement(element.getGroup());
            for (final String property : elementDefinition.getProperties()) {
                cleanElement.putProperty(property, element.getProperty(property));
            }

            return cleanElement;
        }
    }
}
