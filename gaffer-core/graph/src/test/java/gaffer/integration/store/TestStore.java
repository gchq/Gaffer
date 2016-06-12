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
package gaffer.integration.store;

import gaffer.data.element.Element;
import gaffer.operation.Operation;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.operation.impl.get.GetAllElements;
import gaffer.operation.impl.get.GetElements;
import gaffer.store.Context;
import gaffer.store.Store;
import gaffer.store.StoreTrait;
import gaffer.store.operation.handler.OperationHandler;
import java.util.Set;

public class TestStore extends Store {
    @Override
    public Set<StoreTrait> getTraits() {
        return null;
    }

    @Override
    protected boolean isValidationRequired() {
        return false;
    }

    @Override
    protected void addAdditionalOperationHandlers() {

    }

    @Override
    protected OperationHandler<GetElements<ElementSeed, Element>, Iterable<Element>> getGetElementsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<GetAllElements<Element>, Iterable<Element>> getGetAllElementsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<? extends GetAdjacentEntitySeeds, Iterable<EntitySeed>> getAdjacentEntitySeedsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<? extends AddElements, Void> getAddElementsHandler() {
        return null;
    }

    @Override
    protected <OUTPUT> OUTPUT doUnhandledOperation(final Operation<?, OUTPUT> operation, final Context context) {
        return null;
    }
}
