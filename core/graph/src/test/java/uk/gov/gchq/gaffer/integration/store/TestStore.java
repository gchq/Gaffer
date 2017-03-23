/*
 * Copyright 2016-2017 Crown Copyright
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
package uk.gov.gchq.gaffer.integration.store;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.user.User;
import java.util.Set;

import static org.mockito.Mockito.mock;

public class TestStore extends Store {
    public static Store mockStore = mock(TestStore.class);

    @Override
    public JobDetail executeJob(final OperationChain<?> operationChain, final User user) throws OperationException {
        return mockStore.executeJob(operationChain, user);
    }

    @Override
    public <OPERATION extends Operation<?, OUTPUT>, OUTPUT> OUTPUT execute(final OPERATION operation, final User user) throws OperationException {
        return mockStore.execute(operation, user);
    }

    @Override
    public <OUTPUT> OUTPUT execute(final OperationChain<OUTPUT> operationChain, final User user) throws OperationException {
        return mockStore.execute(operationChain, user);
    }

    @Override
    public <OUTPUT> OUTPUT _execute(final OperationChain<OUTPUT> operationChain, final Context context) throws OperationException {
        return mockStore._execute(operationChain, context);
    }

    @Override
    public Set<StoreTrait> getTraits() {
        return mockStore.getTraits();
    }

    @Override
    public boolean isValidationRequired() {
        return mockStore.isValidationRequired();
    }

    @Override
    protected void addAdditionalOperationHandlers() {
    }

    @Override
    protected OperationHandler<GetElements<ElementSeed, Element>, CloseableIterable<Element>> getGetElementsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<GetAllElements<Element>, CloseableIterable<Element>> getGetAllElementsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<? extends GetAdjacentEntitySeeds, CloseableIterable<EntitySeed>> getAdjacentEntitySeedsHandler() {
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
