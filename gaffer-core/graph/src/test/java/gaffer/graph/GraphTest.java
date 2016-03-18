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

import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.operation.Operation;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.operation.impl.get.GetElements;
import gaffer.store.Store;
import gaffer.store.StoreProperties;
import gaffer.store.StoreTrait;
import gaffer.store.operation.handler.OperationHandler;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaEdgeDefinition;
import gaffer.store.schema.SchemaEntityDefinition;
import gaffer.store.schema.TypeDefinition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class GraphTest {
    @Test
    public void shouldConstructGraphFromSchemaModules() {
        // Given
        final StoreProperties storeProperties = new StoreProperties(StoreImpl.class);
        final Schema schemaModule1 = new Schema.Builder()
                .type("prop.string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "prop.string")
                        .build())
                .buildModule();

        final Schema schemaModule2 = new Schema.Builder()
                .type("prop.integer", new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_2, "prop.integer")
                        .build())
                .buildModule();

        final Schema schemaModule3 = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "prop.string")
                        .build())
                .buildModule();

        final Schema schemaModule4 = new Schema.Builder()
                .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_2, "prop.integer")
                        .build())
                .buildModule();


        // When
        final Graph graph = new Graph(storeProperties,
                schemaModule1, schemaModule2, schemaModule3, schemaModule4);

        // Then
        final Schema schema = graph.getSchema();
        schema.getEntity(TestGroups.ENTITY);

    }

    @Test
    public void shouldConstructGraphAndCreateViewWithGroups() {
        // Given
        final Store store = mock(Store.class);
        final Schema schema = mock(Schema.class);
        given(store.getSchema()).willReturn(schema);
        final Set<String> edgeGroups = new HashSet<>();
        edgeGroups.add("edge1");
        edgeGroups.add("edge2");
        edgeGroups.add("edge3");
        edgeGroups.add("edge4");
        given(schema.getEdgeGroups()).willReturn(edgeGroups);

        final Set<String> entityGroups = new HashSet<>();
        entityGroups.add("entity1");
        entityGroups.add("entity2");
        entityGroups.add("entity3");
        entityGroups.add("entity4");
        given(schema.getEntityGroups()).willReturn(entityGroups);

        // When
        final View resultView = new Graph(store).getView();

        // Then
        assertNotSame(schema, resultView);
        assertArrayEquals(entityGroups.toArray(), resultView.getEntityGroups().toArray());
        assertArrayEquals(edgeGroups.toArray(), resultView.getEdgeGroups().toArray());

        for (ViewElementDefinition resultElementDef : resultView.getEntities().values()) {
            assertNotNull(resultElementDef);
            assertEquals(0, resultElementDef.getTransientProperties().size());
            assertNull(resultElementDef.getTransformer());
        }
        for (ViewElementDefinition resultElementDef : resultView.getEdges().values()) {
            assertNotNull(resultElementDef);
            assertEquals(0, resultElementDef.getTransientProperties().size());
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

    static class StoreImpl extends Store {

        @Override
        protected Collection<StoreTrait> getTraits() {
            return new ArrayList<>(0);
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
        protected OperationHandler<? extends GetAdjacentEntitySeeds, Iterable<EntitySeed>> getAdjacentEntitySeedsHandler() {
            return null;
        }

        @Override
        protected OperationHandler<? extends AddElements, Void> getAddElementsHandler() {
            return null;
        }

        @Override
        protected <OUTPUT> OUTPUT doUnhandledOperation(final Operation<?, OUTPUT> operation) {
            return null;
        }
    }
}