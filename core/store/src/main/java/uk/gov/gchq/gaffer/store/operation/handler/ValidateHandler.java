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

package uk.gov.gchq.gaffer.store.operation.handler;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Validate;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.ValidatedElements;

/**
 * An {@code ValidateHandler} handles for {@link uk.gov.gchq.gaffer.operation.impl.Validate} operations.
 * Takes an {@link Iterable} of {@link Element}s and returns an
 * {@link Iterable} containing only valid {@link Element}s, specifically an instance of {@link ValidatedElements}.
 * The {@link uk.gov.gchq.gaffer.store.schema.Schema} is used to validate the elements.
 * The isSkipInvalidElements flag on {@link Validate} is used to determine what to do with invalid {@link Element}s.
 */
public class ValidateHandler implements OutputOperationHandler<Validate, Iterable<? extends Element>> {
    @Override
    public Iterable<? extends Element> doOperation(final Validate operation,
                                                   final Context context, final Store store)
            throws OperationException {
        if (null == operation.getInput()) {
            return null;
        }

        return (Iterable) new ValidatedElements(operation.getInput(), store.getSchema(), operation.isSkipInvalidElements());
    }
}
