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

package gaffer.rest.service;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import gaffer.commonutil.StreamUtil;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.graph.Graph;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.rest.GraphFactory;
import gaffer.rest.userrole.UserRoleHelper;
import gaffer.store.Store;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;


public class SimpleOperationServiceTest {
    @Before
    public void setup() throws IOException {
        System.getProperties().load(StreamUtil.openStream(getClass(), "/rest.properties"));
        System.getProperties().load(StreamUtil.openStream(getClass(), "/roles.properties"));
    }

    @Test
    public void shouldValidateUserRolesOnOperationChainBeforeExecution() throws OperationException {
        // Given
        final GraphFactory graphFactory = mock(GraphFactory.class);
        final UserRoleHelper userRoleHelper = mock(UserRoleHelper.class);
        final SimpleOperationService service = new SimpleOperationService(graphFactory, userRoleHelper);
        final Store store = mock(Store.class);
        final Graph graph = new Graph.Builder()
                .store(store)
                .view(new View())
                .build();

        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds())
                .then(new GetRelatedElements())
                .build();

        given(graphFactory.getGraph()).willReturn(graph);
        doThrow(new IllegalAccessError()).when(userRoleHelper).checkRoles(opChain);

        // When / Then
        try {
            service.execute(opChain);
            fail("Exception expected");
        } catch (final IllegalAccessError e) {
            assertNotNull(e);
        }

        verify(store, never()).execute(opChain);
    }

    @Test
    public void shouldExecuteOpChainOnGraphWhenUserHasRoles() throws OperationException {
        // Given
        final GraphFactory graphFactory = mock(GraphFactory.class);
        final UserRoleHelper userRoleHelper = mock(UserRoleHelper.class);
        final Iterable<Element> expectedResult = mock(Iterable.class);
        final SimpleOperationService service = new SimpleOperationService(graphFactory, userRoleHelper);
        final Store store = mock(Store.class);
        final Graph graph = new Graph.Builder()
                .store(store)
                .view(new View())
                .build();

        final OperationChain<Iterable<Element>> opChain = new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds())
                .then(new GetRelatedElements<EntitySeed, Element>())
                .build();

        given(graphFactory.getGraph()).willReturn(graph);
        given(store.execute(opChain)).willReturn(expectedResult);
        // When
        final Iterable<Element> result = (Iterable<Element>) service.execute(opChain);

        // Then
        assertSame(expectedResult, result);
        verify(userRoleHelper).checkRoles(opChain);
    }
}
