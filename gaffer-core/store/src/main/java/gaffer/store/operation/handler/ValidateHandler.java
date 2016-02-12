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

package gaffer.store.operation.handler;

import gaffer.data.ValidatedElements;
import gaffer.data.element.Element;
import gaffer.operation.OperationException;
import gaffer.operation.impl.Validate;
import gaffer.store.Store;

/**
 * An <code>ValidateHandler</code> handles for {@link gaffer.operation.impl.Validate} operations.
 * Takes an {@link Iterable} of {@link Element}s and returns an
 * {@link Iterable} containing only valid {@link Element}s, specifically an instance of {@link ValidatedElements}.
 * The {@link gaffer.data.elementdefinition.schema.DataSchema} is used to validate the elements.
 * The isSkipInvalidElements flag on {@link Validate} is used to determine what to do with invalid {@link Element}s.
 */
public class ValidateHandler implements OperationHandler<Validate, Iterable<Element>> {
    @Override
    public Iterable<Element> doOperation(final Validate operation, final Store store) throws OperationException {
        return null != operation.getElements()
                ? new ValidatedElements(operation.getElements(), store.getDataSchema(), operation.isSkipInvalidElements())
                : null;
    }
}
