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

package uk.gov.gchq.gaffer.store.operation.handler.generate;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

/**
 * An <code>GenerateElementsHandler</code> handles {@link uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements} operations.
 * It uses the {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} from the operation to generate
 * {@link uk.gov.gchq.gaffer.data.element.Element}s from the operation input objects.
 *
 * @param <OBJ> the type of input objects from the operation.
 */
public class GenerateElementsHandler<OBJ> implements OperationHandler<GenerateElements<OBJ>, CloseableIterable<Element>> {
    @Override
    public CloseableIterable<Element> doOperation(final GenerateElements<OBJ> operation,
                                                  final Context context, final Store store)
            throws OperationException {
        return new WrappedCloseableIterable<>(operation.getElementGenerator().getElements(operation.getObjects()));
    }
}
