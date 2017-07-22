/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation.handler;

import static org.mockito.BDDMockito.given;

import org.junit.Test;
import org.mockito.Mockito;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.Graph.Builder;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.util.LinkedHashSet;

public class FederatedOperationOutputHandlerExceptionTest {

    @Test(expected = OperationException.class)
    public void shouldThrowException() throws Exception {

        FederatedOperationHandler opHandler = new FederatedOperationHandler();

        GetAllElements mockOp = Mockito.mock(GetAllElements.class);
        User testUser = new User("TestUser");
        Context testContext = new Context(testUser);


        Store mockStore = Mockito.mock(Store.class);
        given(mockStore.getSchema()).willReturn(new Schema.Builder().build());
        given(mockStore.execute(new OperationChain<>(mockOp), testUser)).willThrow(Exception.class);

        Graph graph = new Builder()
                .store(mockStore)
                .build();
        LinkedHashSet<Graph> graphs = new LinkedHashSet<>();
        graphs.add(graph);

        FederatedStore mockFederatedStore = Mockito.mock(FederatedStore.class);
        given(mockFederatedStore.getGraphs()).willReturn(graphs);

        opHandler.doOperation(mockOp, testContext, mockFederatedStore);

    }
}