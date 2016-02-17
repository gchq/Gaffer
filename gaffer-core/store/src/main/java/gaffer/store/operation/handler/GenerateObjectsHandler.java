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

import gaffer.data.element.Element;
import gaffer.operation.OperationException;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.store.Store;

/**
 * An <code>GenerateObjectsHandler</code> handles {@link gaffer.operation.impl.generate.GenerateObjects} operations.
 * It uses the {@link gaffer.data.generator.ElementGenerator} from the operation to generate
 * objects of type OBJ from the operation input {@link gaffer.data.element.Element}s.
 *
 * @param <OBJ> the type of output objects from the operation.
 */
public class GenerateObjectsHandler<OBJ> implements OperationHandler<GenerateObjects<Element, OBJ>, Iterable<OBJ>> {
    @Override
    public Iterable<OBJ> doOperation(final GenerateObjects<Element, OBJ> operation, final Store store) throws OperationException {
        return operation.getElementGenerator().getObjects(operation.getElements());
    }
}
