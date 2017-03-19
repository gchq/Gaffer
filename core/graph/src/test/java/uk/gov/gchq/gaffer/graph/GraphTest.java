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

package uk.gov.gchq.gaffer.graph;

import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.aggregate.StringConcat;
import uk.gov.gchq.gaffer.function.aggregate.Sum;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
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
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class GraphTest {
    @Test
    public void shouldConstructGraphFromSchemaModules() {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(StoreImpl.class.getName());

        final Schema schemaModule1 = new Schema.Builder()
                .type(TestTypes.PROP_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                        .build())
                .build();

        final Schema schemaModule2 = new Schema.Builder()
                .type(TestTypes.PROP_INTEGER, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .build())
                .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_2, TestTypes.PROP_INTEGER)
                        .build())
                .build();

        final Schema schemaModule3 = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                        .build())
                .build();

        final Schema schemaModule4 = new Schema.Builder()
                .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_2, TestTypes.PROP_INTEGER)
                        .build())
                .build();


        // When
        final Graph graph = new Graph.Builder()
                .storeProperties(storeProperties)
                .addSchema(schemaModule1)
                .addSchema(schemaModule2)
                .addSchema(schemaModule3)
                .addSchema(schemaModule4)
                .build();

        // Then
        final Schema schema = graph.getSchema();
        schema.getEntity(TestGroups.ENTITY);
    }

    @Test
    public void shouldConstructGraphFromSchemaFolderPath() throws IOException {
        // Given
        final Schema expectedSchema = new Schema.Builder()
                .json(StreamUtil.dataSchema(getClass()), StreamUtil.dataTypes(getClass()))
                .build();

        Graph graph = null;
        File schemaDir = null;
        try {
            schemaDir = createSchemaDirectory();

            // When
            graph = new Graph.Builder()
                    .storeProperties(StreamUtil.storeProps(getClass()))
                    .addSchema(Paths.get(schemaDir.getPath()))
                    .build();
        } finally {
            if (null != schemaDir) {
                FileUtils.deleteDirectory(schemaDir);
            }
        }

        // Then
        JsonUtil.assertEquals(expectedSchema.toJson(true), graph.getSchema().toJson(true));
    }

    @Test
    public void shouldCallAllGraphHooksBeforeOperationExecuted() throws OperationException {
        // Given
        final Operation operation = mock(Operation.class);
        final OperationChain opChain = mock(OperationChain.class);
        given(opChain.getOperations()).willReturn(Collections.singletonList(operation));

        final User user = mock(User.class);
        final GraphHook hook1 = mock(GraphHook.class);
        final GraphHook hook2 = mock(GraphHook.class);
        final Graph graph = new Graph.Builder()
                .storeProperties(StreamUtil.storeProps(getClass()))
                .addSchema(new Schema.Builder().build())
                .addHook(hook1)
                .addHook(hook2)
                .build();

        // When
        graph.execute(opChain, user);

        // Then
        final ArgumentCaptor<OperationChain> captor1 = ArgumentCaptor.forClass(OperationChain.class);
        final ArgumentCaptor<OperationChain> captor2 = ArgumentCaptor.forClass(OperationChain.class);
        final InOrder inOrder = inOrder(hook1, hook2);
        inOrder.verify(hook1).preExecute(captor1.capture(), Mockito.eq(user));
        inOrder.verify(hook2).preExecute(captor2.capture(), Mockito.eq(user));
        assertSame(captor1.getValue(), captor2.getValue());
        final List<Operation> ops = captor1.getValue().getOperations();
        assertEquals(1, ops.size());
        assertSame(operation, ops.get(0));
    }

    @Test
    public void shouldCallAllGraphHooksBeforeOperationChainExecuted() throws OperationException {
        // Given
        final OperationChain opChain = mock(OperationChain.class);
        given(opChain.getOperations()).willReturn(Collections.singletonList(mock(Operation.class)));

        final User user = mock(User.class);
        final GraphHook hook1 = mock(GraphHook.class);
        final GraphHook hook2 = mock(GraphHook.class);
        final Graph graph = new Graph.Builder()
                .storeProperties(StreamUtil.storeProps(getClass()))
                .addSchema(new Schema.Builder().build())
                .addHook(hook1)
                .addHook(hook2)
                .build();

        // When
        graph.execute(opChain, user);

        // Then
        final InOrder inOrder = inOrder(hook1, hook2);
        inOrder.verify(hook1).preExecute(opChain, user);
        inOrder.verify(hook2).preExecute(opChain, user);
    }

    @Test
    public void shouldCallAllGraphHooksBeforeJobExecuted() throws OperationException {
        // Given
        final OperationChain opChain = mock(OperationChain.class);
        given(opChain.getOperations()).willReturn(Collections.singletonList(mock(Operation.class)));

        final User user = mock(User.class);
        final GraphHook hook1 = mock(GraphHook.class);
        final GraphHook hook2 = mock(GraphHook.class);
        final Graph graph = new Graph.Builder()
                .storeProperties(StreamUtil.storeProps(getClass()))
                .addSchema(new Schema.Builder().build())
                .addHook(hook1)
                .addHook(hook2)
                .build();

        // When
        graph.executeJob(opChain, user);

        // Then
        final InOrder inOrder = inOrder(hook1, hook2);
        inOrder.verify(hook1).preExecute(opChain, user);
        inOrder.verify(hook2).preExecute(opChain, user);
    }

    @Test
    public void shouldCallAllGraphHooksAfterOperationExecuted() throws OperationException {
        // Given
        final Operation operation = mock(Operation.class);
        final OperationChain opChain = mock(OperationChain.class);
        given(opChain.getOperations()).willReturn(Collections.singletonList(operation));

        final User user = mock(User.class);
        final GraphHook hook1 = mock(GraphHook.class);
        final GraphHook hook2 = mock(GraphHook.class);
        final Store store = mock(Store.class);
        final Object result1 = mock(Object.class);
        final Object result2 = mock(Object.class);
        final Object result3 = mock(Object.class);
        final Schema schema = new Schema();

        given(store.getSchema()).willReturn(schema);
        given(hook1.postExecute(result1, opChain, user)).willReturn(result2);
        given(hook2.postExecute(result2, opChain, user)).willReturn(result3);

        final Graph graph = new Graph.Builder()
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(schema)
                .addHook(hook1)
                .addHook(hook2)
                .build();

        final ArgumentCaptor<OperationChain> captor = ArgumentCaptor.forClass(OperationChain.class);
        given(store.execute(captor.capture(), Mockito.eq(user))).willReturn(result1);

        // When
        final Object actualResult = graph.execute(opChain, user);

        // Then
        final InOrder inOrder = inOrder(hook1, hook2);
        inOrder.verify(hook1).postExecute(result1, captor.getValue(), user);
        inOrder.verify(hook2).postExecute(result2, captor.getValue(), user);
        final List<Operation> ops = captor.getValue().getOperations();
        assertEquals(1, ops.size());
        assertSame(operation, ops.get(0));
        assertSame(actualResult, result3);
    }

    @Test
    public void shouldCallAllGraphHooksAfterOperationChainExecuted() throws OperationException {
        // Given
        final User user = mock(User.class);
        final GraphHook hook1 = mock(GraphHook.class);
        final GraphHook hook2 = mock(GraphHook.class);
        final Store store = mock(Store.class);
        final Schema schema = new Schema();
        final Object result1 = mock(Object.class);
        final Object result2 = mock(Object.class);
        final Object result3 = mock(Object.class);
        final OperationChain opChain = mock(OperationChain.class);

        given(store.getSchema()).willReturn(schema);
        given(hook1.postExecute(result1, opChain, user)).willReturn(result2);
        given(hook2.postExecute(result2, opChain, user)).willReturn(result3);

        final Graph graph = new Graph.Builder()
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(schema)
                .addHook(hook1)
                .addHook(hook2)
                .build();

        given(opChain.getOperations()).willReturn(Collections.singletonList(mock(Operation.class)));
        given(store.execute(opChain, user)).willReturn(result1);

        // When
        final Object actualResult = graph.execute(opChain, user);

        // Then
        final InOrder inOrder = inOrder(hook1, hook2);
        inOrder.verify(hook1).postExecute(result1, opChain, user);
        inOrder.verify(hook2).postExecute(result2, opChain, user);
        assertSame(actualResult, result3);
    }

    @Test
    public void shouldCallAllGraphHooksAfterJobExecuted() throws OperationException {
        // Given
        final User user = mock(User.class);
        final GraphHook hook1 = mock(GraphHook.class);
        final GraphHook hook2 = mock(GraphHook.class);
        final Store store = mock(Store.class);
        final Schema schema = new Schema();
        final JobDetail result1 = mock(JobDetail.class);
        final JobDetail result2 = mock(JobDetail.class);
        final JobDetail result3 = mock(JobDetail.class);
        final OperationChain opChain = mock(OperationChain.class);

        given(store.getSchema()).willReturn(schema);
        given(hook1.postExecute(result1, opChain, user)).willReturn(result2);
        given(hook2.postExecute(result2, opChain, user)).willReturn(result3);

        final Graph graph = new Graph.Builder()
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(schema)
                .addHook(hook1)
                .addHook(hook2)
                .build();

        given(opChain.getOperations()).willReturn(Collections.singletonList(mock(Operation.class)));
        given(store.executeJob(opChain, user)).willReturn(result1);

        // When
        final JobDetail actualResult = graph.executeJob(opChain, user);

        // Then
        final InOrder inOrder = inOrder(hook1, hook2);
        inOrder.verify(hook1).postExecute(result1, opChain, user);
        inOrder.verify(hook2).postExecute(result2, opChain, user);
        assertSame(actualResult, result3);
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
        final View resultView = new Graph.Builder()
                .store(store)
                .build()
                .getView();

        // Then
        assertNotSame(schema, resultView);
        assertArrayEquals(entityGroups.toArray(), resultView.getEntityGroups().toArray());
        assertArrayEquals(edgeGroups.toArray(), resultView.getEdgeGroups().toArray());

        for (final ViewElementDefinition resultElementDef : resultView.getEntities().values()) {
            assertNotNull(resultElementDef);
            assertEquals(0, resultElementDef.getTransientProperties().size());
            assertNull(resultElementDef.getTransformer());
        }
        for (final ViewElementDefinition resultElementDef : resultView.getEdges().values()) {
            assertNotNull(resultElementDef);
            assertEquals(0, resultElementDef.getTransientProperties().size());
            assertNull(resultElementDef.getTransformer());
        }
    }


    @Test
    public void shouldExposeGetTraitsMethod() throws OperationException {
        // Given
        final Store store = mock(Store.class);
        final View view = mock(View.class);
        final Graph graph = new Graph.Builder()
                .store(store)
                .view(view)
                .build();


        // When
        final Set<StoreTrait> storeTraits = new HashSet<>(Arrays.asList(StoreTrait.STORE_AGGREGATION, StoreTrait.TRANSFORMATION));
        given(store.getTraits()).willReturn(storeTraits);
        final Collection<StoreTrait> returnedTraits = graph.getStoreTraits();

        // Then
        assertEquals(returnedTraits, storeTraits);

    }

    @Test
    public void shouldSetGraphViewOnOperationAndDelegateDoOperationToStore
            () throws OperationException {
        // Given
        final Store store = mock(Store.class);
        final View view = mock(View.class);
        final Graph graph = new Graph.Builder()
                .store(store)
                .view(view)
                .build();
        final User user = new User();
        final int expectedResult = 5;
        final Operation<?, Integer> operation = mock(Operation.class);
        given(operation.getView()).willReturn(null);

        final OperationChain<Integer> opChain = mock(OperationChain.class);
        given(opChain.getOperations()).willReturn(Collections.<Operation>singletonList(operation));
        given(store.execute(opChain, user)).willReturn(expectedResult);

        // When
        int result = graph.execute(opChain, user);

        // Then
        assertEquals(expectedResult, result);
        verify(store).execute(opChain, user);
        verify(operation).setView(view);
    }

    @Test
    public void shouldNotSetGraphViewOnOperationWhenOperationViewIsNotNull
            () throws OperationException {
        // Given
        final Store store = mock(Store.class);
        final View opView = mock(View.class);
        final View view = mock(View.class);
        final Graph graph = new Graph.Builder()
                .store(store)
                .view(view)
                .build();
        final User user = new User();
        final int expectedResult = 5;
        final Operation<?, Integer> operation = mock(Operation.class);
        given(operation.getView()).willReturn(opView);

        final OperationChain<Integer> opChain = mock(OperationChain.class);
        given(opChain.getOperations()).willReturn(Collections.<Operation>singletonList(operation));
        given(store.execute(opChain, user)).willReturn(expectedResult);

        // When
        int result = graph.execute(opChain, user);

        // Then
        assertEquals(expectedResult, result);
        verify(store).execute(opChain, user);
        verify(operation, Mockito.never()).setView(view);
    }

    @Test
    public void shouldThrowExceptionIfStoreClassPropertyIsNotSet() throws OperationException {
        try {
            new Graph.Builder()
                    .addSchema(new Schema())
                    .storeProperties(new StoreProperties())
                    .build();
            fail("exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("The Store class name was not found in the store properties for key: " + StoreProperties.STORE_CLASS, e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionIfSchemaIsInvalid() throws OperationException {
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(StoreImpl.class.getName());
        try {
            new Graph.Builder()
                    .addSchema(new Schema.Builder()
                            .type("intnoagg", new TypeDefinition.Builder()
                                    .clazz(Integer.class)
                                    .build())
                            .type("int", new TypeDefinition.Builder()
                                    .clazz(Integer.class)
                                    .aggregateFunction(new Sum())
                                    .build())
                            .type("string", new TypeDefinition.Builder()
                                    .clazz(String.class)
                                    .aggregateFunction(new StringConcat())
                                    .build())
                            .type("boolean", Boolean.class)
                            .edge("EDGE", new SchemaEdgeDefinition.Builder()
                                    .source("string")
                                    .destination("string")
                                    .directed("boolean")
                                    .property("p", "intnoagg")
                                    .build())
                            .entity("ENTITY", new SchemaEntityDefinition.Builder()
                                    .vertex("string")
                                    .property("p2", "int")
                                    .build())
                            .build())
                    .storeProperties(storeProperties)
                    .build();
            fail("exception expected");
        } catch (final SchemaException e) {
            assertNotNull(e.getMessage());
        }
    }

    static class StoreImpl extends Store {

        @Override
        public Set<StoreTrait> getTraits() {
            return new HashSet<>(0);
        }

        @Override
        public boolean isValidationRequired() {
            return false;
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

    private File createSchemaDirectory() throws IOException {
        final File tmpDir;
        tmpDir = new File("tmpSchemaDir");
        assumeTrue("Failed to create tmp directory, skipping as this test as it is a permissions issue.", tmpDir.mkdir());
        writeToFile("dataSchema.json", tmpDir);
        writeToFile("dataTypes.json", tmpDir);
        return tmpDir;
    }

    private void writeToFile(final String schemaFile, final File dir) throws IOException {
        Files.copy(new SchemaStreamSupplier(schemaFile), new File(dir + "/" + schemaFile));
    }

    private static final class SchemaStreamSupplier implements InputSupplier<InputStream> {
        private final String schemaFile;

        private SchemaStreamSupplier(final String schemaFile) {
            this.schemaFile = schemaFile;
        }

        @Override
        public InputStream getInput() throws IOException {
            return StreamUtil.openStream(getClass(), "/schema/" + schemaFile);
        }
    }
}