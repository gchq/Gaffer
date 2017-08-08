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
import static org.mockito.Mockito.verify;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.util.LinkedHashSet;

public abstract class FederatedOperationOutputHandlerTest<OP extends Output<O>, O> {
    public static final String TEST_ENTITY = "TestEntity";
    public static final String TEST_USER = "testUser";
    public static final String PROPERTY_TYPE = "property";
    protected O o1;
    protected O o2;
    protected O o3;
    protected O o4;
    protected User user;

    @Before
    public void setUp() throws Exception {
        user = new User(TEST_USER);
    }

    @Test
    public void shouldBeSetUp() throws Exception {
        Assert.assertNotNull("Required field object o1 is null", o1);
        Assert.assertNotNull("Required field object o2 is null", o2);
        Assert.assertNotNull("Required field object o3 is null", o3);
        Assert.assertNotNull("Required field object o4 is null", o4);
    }

    protected abstract FederatedOperationOutputHandler<OP, O> getFederatedHandler();

    protected abstract OP getExampleOperation();

    @Test
    final public void shouldMergeResultsFromFieldObjects() throws Exception {
        // Given
        final OP op = getExampleOperation();

        Schema unusedSchema = new Schema.Builder().build();

        Store mockStore1 = getMockStore(op, unusedSchema, o1);
        Store mockStore2 = getMockStore(op, unusedSchema, o2);
        Store mockStore3 = getMockStore(op, unusedSchema, o3);
        Store mockStore4 = getMockStore(op, unusedSchema, o4);

        FederatedStore mockStore = Mockito.mock(FederatedStore.class);
        LinkedHashSet<Graph> linkedGraphs = Sets.newLinkedHashSet();
        linkedGraphs.add(getGraphWithMockStore(mockStore1));
        linkedGraphs.add(getGraphWithMockStore(mockStore2));
        linkedGraphs.add(getGraphWithMockStore(mockStore3));
        linkedGraphs.add(getGraphWithMockStore(mockStore4));
        Mockito.when(mockStore.getGraphs()).thenReturn(linkedGraphs);

        // When
        O theMergedResultsOfOperation = getFederatedHandler().doOperation(op, new Context(user), mockStore);

        //Then
        validateMergeResultsFromFieldObjects(theMergedResultsOfOperation);
        verify(mockStore1).execute(new OperationChain<>(op), user);
        verify(mockStore2).execute(new OperationChain<>(op), user);
        verify(mockStore3).execute(new OperationChain<>(op), user);
        verify(mockStore4).execute(new OperationChain<>(op), user);
    }


    protected abstract boolean validateMergeResultsFromFieldObjects(final O result);

    private Graph getGraphWithMockStore(final Store mockStore) throws uk.gov.gchq.gaffer.operation.OperationException {
        return new Graph.Builder()
                .graphId("TestGraphId")
                .store(mockStore)
                .build();
    }


    private Store getMockStore(final OP op, final Schema unusedSchema, final O willReturn) throws uk.gov.gchq.gaffer.operation.OperationException {
        Store mockStore1 = Mockito.mock(Store.class);
        given(mockStore1.getSchema()).willReturn(unusedSchema);
        given(mockStore1.execute(new OperationChain<>(op), user)).willReturn(willReturn);
        return mockStore1;
    }


}
