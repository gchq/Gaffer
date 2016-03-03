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

package gaffer.graph;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.operation.Operation;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.store.Store;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import java.util.HashSet;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class GraphTest {
    @Test
    public void shouldConstructGraphAndCreateViewWithGroups() {
        // Given
        final Store store = mock(Store.class);
        final DataSchema graphDataSchema = mock(DataSchema.class);
        given(store.getDataSchema()).willReturn(graphDataSchema);
        final Set<String> edgeGroups = new HashSet<>();
        edgeGroups.add("edge1");
        edgeGroups.add("edge2");
        edgeGroups.add("edge3");
        edgeGroups.add("edge4");
        given(graphDataSchema.getEdgeGroups()).willReturn(edgeGroups);

        final Set<String> entityGroups = new HashSet<>();
        entityGroups.add("entity1");
        entityGroups.add("entity2");
        entityGroups.add("entity3");
        entityGroups.add("entity4");
        given(graphDataSchema.getEntityGroups()).willReturn(entityGroups);

        // When
        final View resultView = new Graph(store).getView();

        // Then
        assertNotSame(graphDataSchema, resultView);
        assertArrayEquals(entityGroups.toArray(), resultView.getEntityGroups().toArray());
        assertArrayEquals(edgeGroups.toArray(), resultView.getEdgeGroups().toArray());

        for (ViewElementDefinition resultElementDef : resultView.getEntities().values()) {
            assertNotNull(resultElementDef);
            assertEquals(0, resultElementDef.getProperties().size());
            assertEquals(0, resultElementDef.getIdentifiers().size());
            assertNull(resultElementDef.getTransformer());
        }
        for (ViewElementDefinition resultElementDef : resultView.getEdges().values()) {
            assertNotNull(resultElementDef);
            assertEquals(0, resultElementDef.getProperties().size());
            assertEquals(0, resultElementDef.getIdentifiers().size());
            assertNull(resultElementDef.getTransformer());
        }
    }

    @Test
    public void shouldSetGraphViewOnOperationAndDelegateDoOperationToStore() throws OperationException {
        // Given
        final Store store = mock(Store.class);
        final View view = mock(View.class);
        final Graph graph = new Graph(store, view);
        final int expectedResult = 5;
        final Operation<?, Integer> operation = mock(Operation.class);
        given(operation.getView()).willReturn(null);

        final OperationChain<Integer> opChain = new OperationChain<>(operation);
        given(store.execute(opChain)).willReturn(expectedResult);

        // When
        int result = graph.execute(opChain);

        // Then
        assertEquals(expectedResult, result);
        verify(store).execute(opChain);
        verify(operation).setView(view);
    }

    @Test
    public void shouldNotSetGraphViewOnOperationWhenOperationViewIsNotNull() throws OperationException {
        // Given
        final Store store = mock(Store.class);
        final View opView = mock(View.class);
        final View view = mock(View.class);
        final Graph graph = new Graph(store, view);
        final int expectedResult = 5;
        final Operation<?, Integer> operation = mock(Operation.class);
        given(operation.getView()).willReturn(opView);

        final OperationChain<Integer> opChain = new OperationChain<>(operation);
        given(store.execute(opChain)).willReturn(expectedResult);

        // When
        int result = graph.execute(opChain);

        // Then
        assertEquals(expectedResult, result);
        verify(store).execute(opChain);
        verify(operation, Mockito.never()).setView(view);
    }
}