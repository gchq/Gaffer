/*
 * Copyright 2016-2024 Crown Copyright
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

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.delete.DeleteElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.DeleteAllData;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.handler.GetTraitsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.Set;

import static org.mockito.Mockito.mock;

public class TestStore extends Store {
    public static Store mockStore = mock(TestStore.class);

    @Override
    public JobDetail executeJob(final OperationChain<?> operationChain, final Context context)
            throws OperationException {
        return mockStore.executeJob(operationChain, context);
    }

    @Override
    public <O> O execute(final OperationChain<O> operationChain, final Context context) throws OperationException {
        return mockStore.execute(operationChain, context);
    }

    @Override
    protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
        return ToBytesSerialiser.class;
    }

    @Override
    protected void addAdditionalOperationHandlers() {
    }

    @Override
    protected OutputOperationHandler<GetElements, Iterable<? extends Element>> getGetElementsHandler() {
        return null;
    }

    @Override
    protected OutputOperationHandler<GetAllElements, Iterable<? extends Element>> getGetAllElementsHandler() {
        return null;
    }

    @Override
    protected OutputOperationHandler<? extends GetAdjacentIds, Iterable<? extends EntityId>> getAdjacentIdsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<? extends DeleteElements> getDeleteElementsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<DeleteAllData> getDeleteAllDataHandler() {
        return null;
    }

    @Override
    protected OutputOperationHandler<GetTraits, Set<StoreTrait>> getGetTraitsHandler() {
        return mock(GetTraitsHandler.class);
    }

    @Override
    public boolean isSupported(final Class<? extends Operation> operationClass) {
        return mockStore.isSupported(operationClass);
    }

    @Override
    public OperationHandler<Operation> getOperationHandler(final Class<? extends Operation> opClass) {
        return mockStore.getOperationHandler(opClass);
    }
}
