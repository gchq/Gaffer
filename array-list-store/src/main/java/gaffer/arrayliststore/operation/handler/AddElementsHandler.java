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

package gaffer.arrayliststore.operation.handler;

import gaffer.operation.impl.add.AddElements;
import gaffer.arrayliststore.ArrayListStore;
import gaffer.data.TransformIterable;
import gaffer.data.element.Element;
import gaffer.operation.OperationException;
import gaffer.store.Store;
import gaffer.store.operation.handler.OperationHandler;
import gaffer.store.schema.StoreElementDefinition;

public class AddElementsHandler implements OperationHandler<AddElements, Void> {
    @Override
    public Void doOperation(final AddElements operation, final Store store) throws OperationException {
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
            final StoreElementDefinition elementDefinition = store.getStoreSchema().getElement(element.getGroup());
            for (String property : elementDefinition.getProperties()) {
                cleanElement.putProperty(property, element.getProperty(property));
            }

            return cleanElement;
        }
    }
}
