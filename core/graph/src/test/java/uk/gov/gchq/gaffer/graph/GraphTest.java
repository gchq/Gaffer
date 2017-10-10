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

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.GlobalViewElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.hook.AddOperationsToChain;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.gaffer.graph.hook.Log4jLogger;
import uk.gov.gchq.gaffer.graph.hook.NamedOperationResolver;
import uk.gov.gchq.gaffer.graph.hook.OperationAuthoriser;
import uk.gov.gchq.gaffer.graph.hook.OperationChainLimiter;
import uk.gov.gchq.gaffer.integration.store.TestStore;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.RawDoubleSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class GraphTest {
    private static final String GRAPH_ID = "graphId";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Before
    public void before() throws Exception {
        TestStore.mockStore = mock(TestStore.class);
    }

    @Test
    public void shouldConstructGraphFromSchemaModules() {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStoreImpl.class.getName());

        final Schema schemaModule1 = new Schema.Builder()
                .type(TestTypes.PROP_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("vertex", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                        .aggregate(false)
                        .source("vertex")
                        .destination("vertex")
                        .build())
                .build();

        final Schema schemaModule2 = new Schema.Builder()
                .type(TestTypes.PROP_INTEGER, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .build())
                .type("vertex2", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_2, TestTypes.PROP_INTEGER)
                        .aggregate(false)
                        .source("vertex2")
                        .destination("vertex2")
                        .build())
                .build();

        final Schema schemaModule3 = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                        .aggregate(false)
                        .vertex("vertex3")
                        .build())
                .type("vertex3", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .build();

        final Schema schemaModule4 = new Schema.Builder()
                .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_2, TestTypes.PROP_INTEGER)
                        .aggregate(false)
                        .vertex("vertex4")
                        .build())
                .type("vertex4", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .build();


        // When
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .description("testDescription")
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(storeProperties)
                .addSchema(schemaModule1)
                .addSchema(schemaModule2)
                .addSchema(schemaModule3)
                .addSchema(schemaModule4)
                .build();

        // Then
        final Schema schema = graph.getSchema();
        schema.getEntity(TestGroups.ENTITY);
        assertEquals("testDescription", graph.getDescription());
    }

    @Test
    public void shouldConstructGraphFromSchemaFolderPath() throws IOException {
        // Given
        final Schema expectedSchema = new Schema.Builder()
                .json(StreamUtil.elementsSchema(getClass()), StreamUtil.typesSchema(getClass()))
                .build();

        Graph graph = null;
        File schemaDir = null;
        try {
            schemaDir = createSchemaDirectory();

            // When
            graph = new Graph.Builder()
                    .config(new GraphConfig.Builder()
                            .graphId(GRAPH_ID)
                            .build())
                    .storeProperties(StreamUtil.storeProps(getClass()))
                    .addSchema(Paths.get(schemaDir.getPath()))
                    .build();
        } finally {
            if (null != schemaDir) {
                FileUtils.deleteDirectory(schemaDir);
            }
        }

        // Then
        JsonAssert.assertEquals(expectedSchema.toJson(true), graph.getSchema().toJson(true));
    }

    @Test
    public void shouldConstructGraphFromSchemaURI() throws IOException, URISyntaxException {
        // Given
        final URI typeInputUri = getResourceUri(StreamUtil.TYPES_SCHEMA);
        final URI schemaInputUri = getResourceUri(StreamUtil.ELEMENTS_SCHEMA);
        final URI storeInputUri = getResourceUri(StreamUtil.STORE_PROPERTIES);
        final Schema expectedSchema = new Schema.Builder()
                .json(StreamUtil.elementsSchema(getClass()), StreamUtil.typesSchema(getClass()))
                .build();
        Graph graph = null;
        File schemaDir = null;

        try {
            schemaDir = createSchemaDirectory();

            // When
            graph = new Graph.Builder()
                    .config(new GraphConfig.Builder()
                            .graphId(GRAPH_ID)
                            .build())
                    .storeProperties(storeInputUri)
                    .addSchemas(typeInputUri, schemaInputUri)
                    .build();
        } finally {
            if (schemaDir != null) {
                FileUtils.deleteDirectory(schemaDir);
            }
        }

        // Then
        JsonAssert.assertEquals(expectedSchema.toJson(true), graph.getSchema().toJson(true));
    }

    private URI getResourceUri(String resource) throws URISyntaxException {
        resource = resource.replaceFirst(Pattern.quote("/"), "");
        final URI resourceURI = getClass().getClassLoader().getResource(resource).toURI();
        if (resourceURI == null)
            fail("Test json file not found: " + resource);
        return resourceURI;
    }

    @Test
    public void shouldCloseAllOperationInputsWhenExceptionIsThrownWhenExecuted() throws OperationException, IOException {
        // Given
        final Operation operation = mock(Operation.class);
        final OperationChain opChain = mock(OperationChain.class);
        final OperationChain clonedOpChain = mock(OperationChain.class);
        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));

        final Exception exception = mock(RuntimeException.class);
        final User user = mock(User.class);
        final Context context = mock(Context.class);
        given(context.getUser()).willReturn(user);
        final Store store = mock(Store.class);
        given(store.createContext(user)).willReturn(context);
        given(store.execute(clonedOpChain, context)).willThrow(exception);
        final Schema schema = new Schema();
        given(store.getSchema()).willReturn(schema);

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(new Schema.Builder().build())
                .build();

        // When / Then
        try {
            graph.execute(opChain, user);
            fail("Exception expected");
        } catch (final Exception e) {
            assertSame(exception, e);
            verify(clonedOpChain).close();
        }
    }

    @Test
    public void shouldCloseAllOperationInputsWhenExceptionIsThrownWhenJobExecuted() throws OperationException, IOException {
        // Given
        final Operation operation = mock(Operation.class);
        final OperationChain opChain = mock(OperationChain.class);
        final OperationChain clonedOpChain = mock(OperationChain.class);
        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));

        final Exception exception = mock(RuntimeException.class);
        final User user = mock(User.class);
        final Context context = mock(Context.class);
        given(context.getUser()).willReturn(user);
        final Store store = mock(Store.class);
        given(store.createContext(user)).willReturn(context);
        given(store.executeJob(clonedOpChain, context)).willThrow(exception);
        final Schema schema = new Schema();
        given(store.getSchema()).willReturn(schema);

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(new Schema.Builder().build())
                .build();

        // When / Then
        try {
            graph.executeJob(opChain, user);
            fail("Exception expected");
        } catch (final Exception e) {
            assertSame(exception, e);
            verify(clonedOpChain).close();
        }
    }

    @Test
    public void shouldCallAllGraphHooksBeforeOperationChainExecuted() throws OperationException {
        // Given
        final GetElements operation = mock(GetElements.class);
        final OperationChain opChain = mock(OperationChain.class);
        final OperationChain clonedOpChain = mock(OperationChain.class);
        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));

        final User user = mock(User.class);
        final Context context = mock(Context.class);
        given(context.getUser()).willReturn(user);
        final Store store = mock(Store.class);
        given(store.createContext(user)).willReturn(context);
        final Schema schema = new Schema();
        given(store.getSchema()).willReturn(schema);
        final GraphHook hook1 = mock(GraphHook.class);
        final GraphHook hook2 = mock(GraphHook.class);
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(hook1)
                        .addHook(hook2)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(new Schema.Builder().build())
                .build();

        // When
        graph.execute(opChain, user);

        // Then
        final ArgumentCaptor<OperationChain> captor1 = ArgumentCaptor.forClass(OperationChain.class);
        final ArgumentCaptor<OperationChain> captor2 = ArgumentCaptor.forClass(OperationChain.class);
        final InOrder inOrder = inOrder(hook1, hook2, operation);
        inOrder.verify(hook1).preExecute(captor1.capture(), Mockito.eq(context));
        inOrder.verify(hook2).preExecute(captor2.capture(), Mockito.eq(context));
        inOrder.verify(operation).setView(Mockito.any(View.class));
        assertSame(captor1.getValue(), captor2.getValue());
        final List<Operation> ops = captor1.getValue().getOperations();
        assertEquals(1, ops.size());
        assertSame(operation, ops.get(0));
    }

    @Test
    public void shouldCallAllGraphHooksBeforeJobExecuted() throws OperationException {
        // Given
        final GetElements operation = mock(GetElements.class);
        final OperationChain opChain = mock(OperationChain.class);
        final OperationChain clonedOpChain = mock(OperationChain.class);
        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));

        final User user = mock(User.class);
        final Context context = mock(Context.class);
        given(context.getUser()).willReturn(user);
        final Store store = mock(Store.class);
        given(store.createContext(user)).willReturn(context);
        final Schema schema = new Schema();
        given(store.getSchema()).willReturn(schema);
        final GraphHook hook1 = mock(GraphHook.class);
        final GraphHook hook2 = mock(GraphHook.class);
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(hook1)
                        .addHook(hook2)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(new Schema.Builder().build())
                .build();

        // When
        graph.executeJob(opChain, user);

        // Then
        final ArgumentCaptor<OperationChain> captor1 = ArgumentCaptor.forClass(OperationChain.class);
        final ArgumentCaptor<OperationChain> captor2 = ArgumentCaptor.forClass(OperationChain.class);
        final InOrder inOrder = inOrder(hook1, hook2, operation);
        inOrder.verify(hook1).preExecute(captor1.capture(), Mockito.eq(context));
        inOrder.verify(hook2).preExecute(captor2.capture(), Mockito.eq(context));
        inOrder.verify(operation).setView(Mockito.any(View.class));
        assertSame(captor1.getValue(), captor2.getValue());
        final List<Operation> ops = captor1.getValue().getOperations();
        assertEquals(1, ops.size());
        assertSame(operation, ops.get(0));
    }

    @Test
    public void shouldCallAllGraphHooksAfterOperationExecuted() throws OperationException {
        // Given
        final Operation operation = mock(Operation.class);
        final OperationChain opChain = mock(OperationChain.class);
        final OperationChain clonedOpChain = mock(OperationChain.class);
        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));

        final User user = mock(User.class);
        final GraphHook hook1 = mock(GraphHook.class);
        final GraphHook hook2 = mock(GraphHook.class);
        final Store store = mock(Store.class);
        final Context context = mock(Context.class);
        given(context.getUser()).willReturn(user);
        given(store.createContext(user)).willReturn(context);
        final Object result1 = mock(Object.class);
        final Object result2 = mock(Object.class);
        final Object result3 = mock(Object.class);
        final Schema schema = new Schema();

        given(store.getSchema()).willReturn(schema);
        given(hook1.postExecute(result1, clonedOpChain, context)).willReturn(result2);
        given(hook2.postExecute(result2, clonedOpChain, context)).willReturn(result3);

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(hook1)
                        .addHook(hook2)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(schema)
                .build();

        final ArgumentCaptor<OperationChain> captor = ArgumentCaptor.forClass(OperationChain.class);
        given(store.execute(captor.capture(), Mockito.eq(context))).willReturn(result1);

        // When
        final Object actualResult = graph.execute(opChain, user);

        // Then
        final InOrder inOrder = inOrder(hook1, hook2);
        inOrder.verify(hook1).postExecute(result1, captor.getValue(), context);
        inOrder.verify(hook2).postExecute(result2, captor.getValue(), context);
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
        final Context context = mock(Context.class);
        given(context.getUser()).willReturn(user);
        given(store.createContext(user)).willReturn(context);
        final Schema schema = new Schema();
        final Object result1 = mock(Object.class);
        final Object result2 = mock(Object.class);
        final Object result3 = mock(Object.class);
        final OperationChain opChain = mock(OperationChain.class);
        final OperationChain clonedOpChain = mock(OperationChain.class);
        given(opChain.shallowClone()).willReturn(clonedOpChain);

        given(store.getSchema()).willReturn(schema);
        given(hook1.postExecute(result1, clonedOpChain, context)).willReturn(result2);
        given(hook2.postExecute(result2, clonedOpChain, context)).willReturn(result3);

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(hook1)
                        .addHook(hook2)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(schema)
                .build();

        given(clonedOpChain.getOperations()).willReturn(Collections.singletonList(mock(Operation.class)));
        given(store.execute(clonedOpChain, context)).willReturn(result1);

        // When
        final Object actualResult = graph.execute(opChain, user);

        // Then
        final InOrder inOrder = inOrder(hook1, hook2);
        inOrder.verify(hook1).postExecute(result1, clonedOpChain, context);
        inOrder.verify(hook2).postExecute(result2, clonedOpChain, context);
        assertSame(actualResult, result3);
    }

    @Test
    public void shouldCallAllGraphHooksAfterJobExecuted() throws OperationException {
        // Given
        final User user = mock(User.class);
        final GraphHook hook1 = mock(GraphHook.class);
        final GraphHook hook2 = mock(GraphHook.class);
        final Store store = mock(Store.class);
        final Context context = mock(Context.class);
        given(context.getUser()).willReturn(user);
        given(store.createContext(user)).willReturn(context);
        final Schema schema = new Schema();
        final JobDetail result1 = mock(JobDetail.class);
        final JobDetail result2 = mock(JobDetail.class);
        final JobDetail result3 = mock(JobDetail.class);
        final OperationChain opChain = mock(OperationChain.class);
        final OperationChain clonedOpChain = mock(OperationChain.class);
        given(opChain.shallowClone()).willReturn(clonedOpChain);

        given(store.getSchema()).willReturn(schema);
        given(hook1.postExecute(result1, clonedOpChain, context)).willReturn(result2);
        given(hook2.postExecute(result2, clonedOpChain, context)).willReturn(result3);

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(hook1)
                        .addHook(hook2)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(schema)
                .build();

        given(clonedOpChain.getOperations()).willReturn(Collections.singletonList(mock(Operation.class)));
        given(store.executeJob(clonedOpChain, context)).willReturn(result1);

        // When
        final JobDetail actualResult = graph.executeJob(opChain, user);

        // Then
        final InOrder inOrder = inOrder(hook1, hook2);
        inOrder.verify(hook1).postExecute(result1, clonedOpChain, context);
        inOrder.verify(hook2).postExecute(result2, clonedOpChain, context);
        assertSame(actualResult, result3);
    }

    @Test
    public void shouldCallAllGraphHooksOnGraphHookPreExecuteFailure() throws OperationException {
        // Given
        final Operation operation = mock(Operation.class);
        final OperationChain opChain = mock(OperationChain.class);
        final OperationChain clonedOpChain = mock(OperationChain.class);
        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));

        final User user = mock(User.class);
        final Context context = mock(Context.class);
        given(context.getUser()).willReturn(user);
        final GraphHook hook1 = mock(GraphHook.class);
        final GraphHook hook2 = mock(GraphHook.class);
        final Store store = mock(Store.class);
        final Schema schema = new Schema();
        given(store.createContext(user)).willReturn(context);
        given(store.getSchema()).willReturn(schema);
        final RuntimeException e = new RuntimeException("Hook2 failed in postExecute");
        doThrow(e).when(hook1).preExecute(clonedOpChain, context);
        given(hook1.onFailure(null, clonedOpChain, context, e)).willThrow(new RuntimeException("Hook1 failed in onFailure"));
        given(hook2.onFailure(null, clonedOpChain, context, e)).willReturn(null);

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(hook1)
                        .addHook(hook2)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(schema)
                .build();

        // When / Then
        try {
            graph.execute(opChain, user);
            fail("Exception expected");
        } catch (final RuntimeException runtimeE) {
            final InOrder inOrder = inOrder(hook1, hook2);
            inOrder.verify(hook2, never()).preExecute(any(), any());
            inOrder.verify(hook1, never()).postExecute(any(), any(), any());
            inOrder.verify(hook2, never()).postExecute(any(), any(), any());
            inOrder.verify(hook1).onFailure(eq(null), any(), eq(context), eq(e));
            inOrder.verify(hook2).onFailure(eq(null), any(), eq(context), eq(e));
        }
    }

    @Test
    public void shouldCallAllGraphHooksOnGraphHookPostExecuteFailure() throws OperationException {
        // Given
        final Operation operation = mock(Operation.class);
        final OperationChain opChain = mock(OperationChain.class);
        final OperationChain clonedOpChain = mock(OperationChain.class);
        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));

        final User user = mock(User.class);
        final GraphHook hook1 = mock(GraphHook.class);
        final GraphHook hook2 = mock(GraphHook.class);
        final Store store = mock(Store.class);
        final Context context = mock(Context.class);
        given(context.getUser()).willReturn(user);
        given(store.createContext(user)).willReturn(context);
        final Object result1 = mock(Object.class);
        final Object result2 = mock(Object.class);
        final Object result3 = mock(Object.class);
        final Schema schema = new Schema();

        given(store.getSchema()).willReturn(schema);
        given(hook1.postExecute(result1, clonedOpChain, context)).willReturn(result2);
        final RuntimeException e = new RuntimeException("Hook2 failed in postExecute");
        given(hook2.postExecute(result2, clonedOpChain, context)).willThrow(e);
        given(hook1.onFailure(result2, clonedOpChain, context, e)).willThrow(new RuntimeException("Hook1 failed in onFailure"));
        given(hook2.onFailure(result2, clonedOpChain, context, e)).willReturn(result3);

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(hook1)
                        .addHook(hook2)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(schema)
                .build();

        final ArgumentCaptor<OperationChain> captor = ArgumentCaptor.forClass(OperationChain.class);
        given(store.execute(captor.capture(), Mockito.eq(context))).willReturn(result1);

        // When / Then
        try {
            graph.execute(opChain, user);
            fail("Exception expected");
        } catch (final RuntimeException runtimeE) {
            final InOrder inOrder = inOrder(hook1, hook2);
            inOrder.verify(hook1).postExecute(result1, captor.getValue(), context);
            inOrder.verify(hook2).postExecute(result2, captor.getValue(), context);
            inOrder.verify(hook1).onFailure(result2, captor.getValue(), context, e);
            inOrder.verify(hook2).onFailure(result2, captor.getValue(), context, e);
            final List<Operation> ops = captor.getValue().getOperations();
            assertEquals(1, ops.size());
            assertSame(operation, ops.get(0));
        }
    }

    @Test
    public void shouldCallAllGraphHooksOnExecuteFailure() throws OperationException {
        // Given
        final Operation operation = mock(Operation.class);
        final OperationChain opChain = mock(OperationChain.class);
        final OperationChain clonedOpChain = mock(OperationChain.class);
        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));

        final User user = mock(User.class);
        final GraphHook hook1 = mock(GraphHook.class);
        final GraphHook hook2 = mock(GraphHook.class);
        final Store store = mock(Store.class);
        final Context context = mock(Context.class);
        given(context.getUser()).willReturn(user);
        given(store.createContext(user)).willReturn(context);
        final Schema schema = new Schema();

        final RuntimeException e = new RuntimeException("Store failed to execute operation chain");

        given(store.getSchema()).willReturn(schema);
        given(hook1.onFailure(null, clonedOpChain, context, e)).willThrow(new RuntimeException("Hook1 failed in onFailure"));
        given(hook2.onFailure(null, clonedOpChain, context, e)).willReturn(null);

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(hook1)
                        .addHook(hook2)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(schema)
                .build();

        final ArgumentCaptor<OperationChain> captor = ArgumentCaptor.forClass(OperationChain.class);
        given(store.execute(captor.capture(), Mockito.eq(context))).willThrow(e);

        // When / Then
        try {
            graph.execute(opChain, user);
            fail("Exception expected");
        } catch (final RuntimeException runtimeE) {
            final InOrder inOrder = inOrder(hook1, hook2);
            inOrder.verify(hook1, never()).postExecute(any(), any(), any());
            inOrder.verify(hook2, never()).postExecute(any(), any(), any());
            inOrder.verify(hook1).onFailure(null, captor.getValue(), context, e);
            inOrder.verify(hook2).onFailure(null, captor.getValue(), context, e);
            final List<Operation> ops = captor.getValue().getOperations();
            assertEquals(1, ops.size());
            assertSame(operation, ops.get(0));
        }
    }

    @Test
    public void shouldCallAllGraphHooksOnGraphHookPreExecuteFailureWhenRunningJob() throws OperationException {
        // Given
        final Operation operation = mock(Operation.class);
        final OperationChain opChain = mock(OperationChain.class);
        final OperationChain clonedOpChain = mock(OperationChain.class);
        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));

        final User user = mock(User.class);
        final Context context = mock(Context.class);
        given(context.getUser()).willReturn(user);
        final GraphHook hook1 = mock(GraphHook.class);
        final GraphHook hook2 = mock(GraphHook.class);
        final Store store = mock(Store.class);
        final Schema schema = new Schema();
        given(store.createContext(user)).willReturn(context);
        given(store.getSchema()).willReturn(schema);
        final RuntimeException e = new RuntimeException("Hook2 failed in postExecute");
        doThrow(e).when(hook1).preExecute(clonedOpChain, context);
        given(hook1.onFailure(null, clonedOpChain, context, e)).willThrow(new RuntimeException("Hook1 failed in onFailure"));
        given(hook2.onFailure(null, clonedOpChain, context, e)).willReturn(null);

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(hook1)
                        .addHook(hook2)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(schema)
                .build();

        // When / Then
        try {
            graph.executeJob(opChain, user);
            fail("Exception expected");
        } catch (final RuntimeException runtimeE) {
            final InOrder inOrder = inOrder(hook1, hook2);
            inOrder.verify(hook2, never()).preExecute(any(), any());
            inOrder.verify(hook1, never()).postExecute(any(), any(), any());
            inOrder.verify(hook2, never()).postExecute(any(), any(), any());
            inOrder.verify(hook1).onFailure(eq(null), any(), eq(context), eq(e));
            inOrder.verify(hook2).onFailure(eq(null), any(), eq(context), eq(e));
        }
    }

    @Test
    public void shouldCallAllGraphHooksOnGraphHookPostExecuteFailureWhenRunningJob() throws OperationException {
        // Given
        final Operation operation = mock(Operation.class);
        final OperationChain opChain = mock(OperationChain.class);
        final OperationChain clonedOpChain = mock(OperationChain.class);
        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));

        final User user = mock(User.class);
        final GraphHook hook1 = mock(GraphHook.class);
        final GraphHook hook2 = mock(GraphHook.class);
        final Store store = mock(Store.class);
        final Context context = mock(Context.class);
        given(context.getUser()).willReturn(user);
        given(store.createContext(user)).willReturn(context);
        final JobDetail result1 = mock(JobDetail.class);
        final JobDetail result2 = mock(JobDetail.class);
        final JobDetail result3 = mock(JobDetail.class);
        final Schema schema = new Schema();

        given(store.getSchema()).willReturn(schema);
        given(hook1.postExecute(result1, clonedOpChain, context)).willReturn(result2);
        final RuntimeException e = new RuntimeException("Hook2 failed in postExecute");
        given(hook2.postExecute(result2, clonedOpChain, context)).willThrow(e);
        given(hook1.onFailure(result2, clonedOpChain, context, e)).willThrow(new RuntimeException("Hook1 failed in onFailure"));
        given(hook2.onFailure(result2, clonedOpChain, context, e)).willReturn(result3);

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(hook1)
                        .addHook(hook2)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(schema)
                .build();

        final ArgumentCaptor<OperationChain> captor = ArgumentCaptor.forClass(OperationChain.class);
        given(store.executeJob(captor.capture(), Mockito.eq(context))).willReturn(result1);

        // When / Then
        try {
            graph.executeJob(opChain, user);
            fail("Exception expected");
        } catch (final RuntimeException runtimeE) {
            final InOrder inOrder = inOrder(hook1, hook2);
            inOrder.verify(hook1).postExecute(result1, captor.getValue(), context);
            inOrder.verify(hook2).postExecute(result2, captor.getValue(), context);
            inOrder.verify(hook1).onFailure(result2, captor.getValue(), context, e);
            inOrder.verify(hook2).onFailure(result2, captor.getValue(), context, e);
            final List<Operation> ops = captor.getValue().getOperations();
            assertEquals(1, ops.size());
            assertSame(operation, ops.get(0));
        }
    }

    @Test
    public void shouldCallAllGraphHooksOnExecuteFailureWhenRunningJob() throws OperationException {
        // Given
        final Operation operation = mock(Operation.class);
        final OperationChain opChain = mock(OperationChain.class);
        final OperationChain clonedOpChain = mock(OperationChain.class);
        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));

        final User user = mock(User.class);
        final GraphHook hook1 = mock(GraphHook.class);
        final GraphHook hook2 = mock(GraphHook.class);
        final Store store = mock(Store.class);
        final Context context = mock(Context.class);
        given(context.getUser()).willReturn(user);
        given(store.createContext(user)).willReturn(context);
        final Schema schema = new Schema();
        final RuntimeException e = new RuntimeException("Store failed to execute operation chain");

        given(store.getSchema()).willReturn(schema);
        given(hook1.onFailure(null, clonedOpChain, context, e)).willThrow(new RuntimeException("Hook1 failed in onFailure"));
        given(hook2.onFailure(null, clonedOpChain, context, e)).willReturn(null);

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(hook1)
                        .addHook(hook2)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(schema)
                .build();

        final ArgumentCaptor<OperationChain> captor = ArgumentCaptor.forClass(OperationChain.class);
        given(store.executeJob(captor.capture(), Mockito.eq(context))).willThrow(e);

        // When / Then
        try {
            graph.executeJob(opChain, user);
            fail("Exception expected");
        } catch (final RuntimeException runtimeE) {
            final InOrder inOrder = inOrder(hook1, hook2);
            inOrder.verify(hook1, never()).postExecute(any(), any(), any());
            inOrder.verify(hook2, never()).postExecute(any(), any(), any());
            inOrder.verify(hook1).onFailure(null, captor.getValue(), context, e);
            inOrder.verify(hook2).onFailure(null, captor.getValue(), context, e);
            final List<Operation> ops = captor.getValue().getOperations();
            assertEquals(1, ops.size());
            assertSame(operation, ops.get(0));
        }
    }

    @Test
    public void shouldConstructGraphAndCreateViewWithGroups() {
        // Given
        final Store store = mock(Store.class);
        given(store.getGraphId()).willReturn(GRAPH_ID);
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
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .view(view)
                        .build())
                .store(store)
                .build();


        // When
        final Set<StoreTrait> storeTraits = new HashSet<>(Arrays.asList(StoreTrait.INGEST_AGGREGATION, StoreTrait.TRANSFORMATION));
        given(store.getTraits()).willReturn(storeTraits);
        final Collection<StoreTrait> returnedTraits = graph.getStoreTraits();

        // Then
        assertEquals(returnedTraits, storeTraits);

    }

    @Test
    public void shouldSetGraphViewOnOperationAndDelegateDoOperationToStore()
            throws OperationException {
        // Given
        final Store store = mock(Store.class);
        final View view = mock(View.class);
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .view(view)
                        .build())
                .store(store)
                .build();
        final User user = new User();
        final Context context = mock(Context.class);
        given(context.getUser()).willReturn(user);
        given(store.createContext(user)).willReturn(context);
        final Integer expectedResult = 5;
        final GetElements operation = mock(GetElements.class);
        given(operation.getView()).willReturn(null);

        final OperationChain<Integer> opChain = mock(OperationChain.class);
        final OperationChain clonedOpChain = mock(OperationChain.class);
        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));
        given(store.execute(clonedOpChain, context)).willReturn(expectedResult);

        // When
        Integer result = graph.execute(opChain, user);

        // Then
        assertEquals(expectedResult, result);
        verify(store).execute(clonedOpChain, context);
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
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .view(view)
                        .build())
                .store(store)
                .build();
        final User user = new User();
        final Context context = mock(Context.class);
        given(context.getUser()).willReturn(user);
        given(store.createContext(user)).willReturn(context);
        final Integer expectedResult = 5;
        final GetElements operation = mock(GetElements.class);
        given(operation.getView()).willReturn(opView);

        final OperationChain<Integer> opChain = mock(OperationChain.class);
        final OperationChain<Integer> clonedOpChain = mock(OperationChain.class);
        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(opChain.getOperations()).willReturn(Lists.newArrayList(operation));
        given(store.execute(clonedOpChain, context)).willReturn(expectedResult);

        // When
        Integer result = graph.execute(opChain, user);

        // Then
        assertEquals(expectedResult, result);
        verify(store).execute(clonedOpChain, context);
        verify(operation, Mockito.never()).setView(view);
    }

    @Test
    public void shouldNotSetGraphViewOnOperationWhenOperationIsNotAGet
            () throws OperationException {
        // Given
        final Store store = mock(Store.class);
        final View view = mock(View.class);
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .view(view)
                        .build())
                .store(store)
                .build();
        final User user = new User();
        final Context context = mock(Context.class);
        given(context.getUser()).willReturn(user);
        given(store.createContext(user)).willReturn(context);
        final int expectedResult = 5;
        final Operation operation = mock(Operation.class);

        final OperationChain<Integer> opChain = mock(OperationChain.class);
        final OperationChain<Integer> clonedOpChain = mock(OperationChain.class);
        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));
        given(store.execute(clonedOpChain, context)).willReturn(expectedResult);

        // When
        int result = graph.execute(opChain, user);

        // Then
        assertEquals(expectedResult, result);
        verify(store).execute(clonedOpChain, context);
    }

    @Test
    public void shouldThrowExceptionIfStoreClassPropertyIsNotSet() throws OperationException {
        try {
            new Graph.Builder()
                    .config(new GraphConfig.Builder()
                            .graphId(GRAPH_ID)
                            .build())
                    .addSchema(new Schema())
                    .storeProperties(new StoreProperties())
                    .build();
            fail("exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("The Store class name was not found in the store properties for key: " + StoreProperties.STORE_CLASS + ", GraphId: " + GRAPH_ID, e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionIfSchemaIsInvalid() throws OperationException {
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStoreImpl.class.getName());
        try {
            new Graph.Builder()
                    .config(new GraphConfig.Builder()
                            .graphId(GRAPH_ID)
                            .build())
                    .addSchema(new Schema.Builder()
                            .type("int", new TypeDefinition.Builder()
                                    .clazz(Integer.class)
                                    .aggregateFunction(new Sum())
                                            // invalid serialiser
                                    .serialiser(new RawDoubleSerialiser())
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

    @Test
    public void shouldDelegateGetNextOperationsToStore() {
        // Given
        final Store store = mock(Store.class);
        given(store.getSchema()).willReturn(new Schema());
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .store(store)
                .build();

        final Set<Class<? extends Operation>> expectedNextOperations = mock(Set.class);
        given(store.getNextOperations(GetElements.class)).willReturn(expectedNextOperations);

        // When
        final Set<Class<? extends Operation>> nextOperations = graph.getNextOperations(GetElements.class);

        // Then
        assertSame(expectedNextOperations, nextOperations);
    }

    @Test
    public void shouldDelegateIsSupportedToStore() {
        // Given
        final Store store = mock(Store.class);
        given(store.getSchema()).willReturn(new Schema());
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .store(store)
                .build();

        given(store.isSupported(GetElements.class)).willReturn(true);
        given(store.isSupported(GetAllElements.class)).willReturn(false);

        // When / Then
        assertTrue(graph.isSupported(GetElements.class));
        assertFalse(graph.isSupported(GetAllElements.class));
    }

    @Test
    public void shouldDelegateGetSupportedOperationsToStore() {
        // Given
        final Store store = mock(Store.class);
        given(store.getSchema()).willReturn(new Schema());
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .store(store)
                .build();

        final Set<Class<? extends Operation>> expectedSupportedOperations = mock(Set.class);
        given(store.getSupportedOperations()).willReturn(expectedSupportedOperations);

        // When
        final Set<Class<? extends Operation>> supportedOperations = graph.getSupportedOperations();

        // Then
        assertSame(expectedSupportedOperations, supportedOperations);
    }

    @Test
    public void shouldThrowExceptionWithInvalidSchema() {

        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStoreImpl.class.getName());

        //When / Then
        try {
            new Graph.Builder()
                    .config(new GraphConfig.Builder()
                            .graphId(GRAPH_ID)
                            .build())
                    .addSchema(new Schema.Builder()
                            .edge("group", new SchemaEdgeDefinition())
                            .entity("group", new SchemaEntityDefinition())
                            .build())
                    .storeProperties(storeProperties)
                    .build();
        } catch (final SchemaException e) {
            assertTrue(e.getMessage().contains("Schema is not valid"));
        }
    }

    @Test
    public void shouldThrowExceptionWithNullSchema() {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStoreImpl.class.getName());

        //When / Then
        try {
            new Graph.Builder()
                    .config(new GraphConfig.Builder()
                            .graphId(GRAPH_ID)
                            .build())
                    .storeProperties(storeProperties)
                    .build();
        } catch (final SchemaException e) {
            assertTrue(e.getMessage().contains("Schema is missing"));
        }
    }

    private File createSchemaDirectory() throws IOException {
        final File tmpDir = tempFolder.newFolder("tmpSchemaDir");
        writeToFile("elements.json", tmpDir);
        writeToFile("types.json", tmpDir);
        return tmpDir;
    }

    private void writeToFile(final String schemaFile, final File dir) throws IOException {
        Files.copy(new SchemaStreamSupplier(schemaFile), new File(dir + "/" + schemaFile));
    }

    @Test
    public void shouldThrowExceptionIfGraphIdIsInvalid() throws Exception {
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        try {
            new Graph.Builder()
                    .config(new GraphConfig.Builder()
                            .graphId("invalid-id")
                            .build())
                    .build();
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldBuildGraphUsingGraphIdAndLookupSchema() throws Exception {
        // Given
        HashMapGraphLibrary.clear();
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStoreImpl.class.getName());

        final Schema schemaModule1 = new Schema.Builder()
                .type(TestTypes.PROP_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("vertex", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                        .aggregate(false)
                        .source("vertex")
                        .destination("vertex")
                        .build())
                .build();

        final Schema schemaModule2 = new Schema.Builder()
                .type(TestTypes.PROP_INTEGER, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .build())
                .type("vertex2", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_2, TestTypes.PROP_INTEGER)
                        .aggregate(false)
                        .source("vertex2")
                        .destination("vertex2")
                        .build())
                .build();

        final Schema schemaModule3 = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                        .aggregate(false)
                        .vertex("vertex3")
                        .build())
                .type("vertex3", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .build();

        final Schema schemaModule4 = new Schema.Builder()
                .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_2, TestTypes.PROP_INTEGER)
                        .aggregate(false)
                        .vertex("vertex4")
                        .build())
                .type("vertex4", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .build();


        // When
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .library(new HashMapGraphLibrary())
                        .build())
                .addSchema(schemaModule1)
                .addSchema(schemaModule2)
                .addSchema(schemaModule3)
                .addSchema(schemaModule4)
                .storeProperties(storeProperties)
                .build();

        final Graph graph2 = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .library(new HashMapGraphLibrary())
                        .build())
                .storeProperties(storeProperties)
                .build();

        // Then
        JsonAssert.assertEquals(graph.getSchema().toJson(false), graph2.getSchema().toJson(false));
    }

    @Test
    public void shouldAddHooksVarArgsAndGetGraphHooks() throws Exception {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStoreImpl.class.getName());
        final GraphHook graphHook1 = mock(GraphHook.class);
        final Log4jLogger graphHook2 = mock(Log4jLogger.class);

        // When
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .addHooks(graphHook1, graphHook2)
                        .build())
                .storeProperties(storeProperties)
                .addSchemas(StreamUtil.schemas(getClass()))
                .build();

        // Then
        assertEquals(Arrays.asList(graphHook1.getClass(), graphHook2.getClass()), graph.getGraphHooks());
    }

    @Test
    public void shouldAddHookAndGetGraphHooks() throws Exception {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStore.class.getName());
        TestStore.mockStore = mock(Store.class);
        given(TestStore.mockStore.isSupported(NamedOperation.class)).willReturn(true);
        final GraphHook graphHook1 = mock(GraphHook.class);
        final NamedOperationResolver graphHook2 = new NamedOperationResolver();
        final Log4jLogger graphHook3 = mock(Log4jLogger.class);

        // When
        final Graph graph = new Graph.Builder()
                .graphId("graphId")
                .storeProperties(storeProperties)
                .addSchemas(StreamUtil.schemas(getClass()))
                .addHook(graphHook1)
                .addHook(graphHook2)
                .addHook(graphHook3)
                .build();

        // Then
        assertEquals(Arrays.asList(graphHook1.getClass(), graphHook2.getClass(), graphHook3.getClass()), graph.getGraphHooks());
    }

    @Test
    public void shouldAddNamedOperationResolverHookFirst() throws Exception {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStore.class.getName());
        TestStore.mockStore = mock(Store.class);
        given(TestStore.mockStore.isSupported(NamedOperation.class)).willReturn(true);
        final GraphHook graphHook1 = mock(GraphHook.class);
        final Log4jLogger graphHook2 = mock(Log4jLogger.class);

        // When
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .addHook(graphHook1)
                        .addHook(graphHook2)
                        .build())
                .storeProperties(storeProperties)
                .addSchemas(StreamUtil.schemas(getClass()))
                .build();

        // Then
        assertEquals(Arrays.asList(NamedOperationResolver.class, graphHook1.getClass(), graphHook2.getClass()), graph.getGraphHooks());
    }

    @Test
    public void shouldAddHooksFromPathAndGetGraphHooks() throws Exception {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStoreImpl.class.getName());

        final File graphHooks = tempFolder.newFile("graphHooks.json");
        FileUtils.writeLines(graphHooks, IOUtils.readLines(StreamUtil.openStream(getClass(), "graphHooks.json")));

        // When
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .addHooks(Paths.get(graphHooks.getPath()))
                        .build())
                .storeProperties(storeProperties)
                .addSchemas(StreamUtil.schemas(getClass()))
                .build();

        // Then
        assertEquals(
                Arrays.asList(OperationChainLimiter.class, AddOperationsToChain.class, OperationAuthoriser.class),
                graph.getGraphHooks()
        );
    }

    @Test
    public void shouldAddHookFromPathAndGetGraphHooks() throws Exception {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStoreImpl.class.getName());

        final File graphHook1File = tempFolder.newFile("opChainLimiter.json");
        FileUtils.writeLines(graphHook1File, IOUtils.readLines(StreamUtil.openStream(getClass(), "opChainLimiter.json")));

        final File graphHook2File = tempFolder.newFile("opAuthoriser.json");
        FileUtils.writeLines(graphHook2File, IOUtils.readLines(StreamUtil.openStream(getClass(), "opAuthoriser.json")));

        // When
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .addHook(Paths.get(graphHook1File.getPath()))
                        .addHook(Paths.get(graphHook2File.getPath()))
                        .build())
                .storeProperties(storeProperties)
                .addSchemas(StreamUtil.schemas(getClass()))
                .build();

        // Then
        assertEquals(
                Arrays.asList(OperationChainLimiter.class, OperationAuthoriser.class),
                graph.getGraphHooks()
        );
    }

    @Test
    public void shouldBuildGraphFromConfigFile() throws Exception {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStoreImpl.class.getName());

        // When
        final Graph graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(getClass()))
                .storeProperties(storeProperties)
                .addSchemas(StreamUtil.schemas(getClass()))
                .build();

        // Then
        assertEquals("graphId1", graph.getGraphId());
        assertEquals(new View.Builder()
                .globalElements(new GlobalViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build(), graph.getView());
        assertEquals(HashMapGraphLibrary.class, graph.getGraphLibrary().getClass());
        assertEquals(Arrays.asList(OperationChainLimiter.class, AddOperationsToChain.class),
                graph.getGraphHooks());
    }

    @Test
    public void shouldBuildGraphFromConfigAndMergeConfigWithExistingConfig() throws Exception {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStoreImpl.class.getName());

        final String graphId1 = "graphId1";
        final String graphId2 = "graphId2";

        final GraphLibrary library1 = mock(GraphLibrary.class);
        final GraphLibrary library2 = mock(GraphLibrary.class);

        final View view1 = mock(View.class);
        final View view2 = mock(View.class);

        final GraphHook hook1 = mock(GraphHook.class);
        final GraphHook hook2 = mock(GraphHook.class);
        final GraphHook hook3 = mock(GraphHook.class);

        // When
        final GraphConfig config = new GraphConfig.Builder()
                .graphId(graphId2)
                .library(library2)
                .addHook(hook2)
                .view(view2)
                .build();

        final Graph graph = new Graph.Builder()
                .graphId(graphId1)
                .library(library1)
                .view(view1)
                .storeProperties(storeProperties)
                .addSchemas(StreamUtil.schemas(getClass()))
                .addHook(hook1)
                .config(config)
                .addHook(hook3)
                .build();

        // Then
        assertEquals(graphId2, graph.getGraphId());
        assertEquals(view2, graph.getView());
        assertEquals(library2, graph.getGraphLibrary());
        assertEquals(Arrays.asList(hook1.getClass(), hook2.getClass(), hook3.getClass()),
                graph.getGraphHooks());
    }

    @Test
    public void shouldBuildGraphFromConfigAndOverrideFields() throws Exception {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStoreImpl.class.getName());

        final String graphId1 = "graphId1";
        final String graphId2 = "graphId2";

        final GraphLibrary library1 = mock(GraphLibrary.class);
        final GraphLibrary library2 = mock(GraphLibrary.class);

        final View view1 = mock(View.class);
        final View view2 = mock(View.class);

        final GraphHook hook1 = mock(GraphHook.class);
        final GraphHook hook2 = mock(GraphHook.class);
        final GraphHook hook3 = mock(GraphHook.class);

        // When
        final GraphConfig config = new GraphConfig.Builder()
                .graphId(graphId2)
                .library(library2)
                .addHook(hook2)
                .view(view2)
                .build();

        final Graph graph = new Graph.Builder()
                .config(config)
                .graphId(graphId1)
                .library(library1)
                .view(view1)
                .storeProperties(storeProperties)
                .addSchemas(StreamUtil.schemas(getClass()))
                .addHook(hook1)
                .addHook(hook3)
                .build();

        // Then
        assertEquals(graphId1, graph.getGraphId());
        assertEquals(view1, graph.getView());
        assertEquals(library1, graph.getGraphLibrary());
        assertEquals(Arrays.asList(hook2.getClass(), hook1.getClass(), hook3.getClass()),
                graph.getGraphHooks());
    }

    public static class TestStoreImpl extends Store {
        @Override
        public Set<StoreTrait> getTraits() {
            return new HashSet<>(0);
        }

        @Override
        protected void addAdditionalOperationHandlers() {
        }

        @Override
        protected OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> getGetElementsHandler() {
            return null;
        }

        @Override
        protected OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> getGetAllElementsHandler() {
            return null;
        }

        @Override
        protected OutputOperationHandler<? extends GetAdjacentIds, CloseableIterable<? extends EntityId>> getAdjacentIdsHandler() {
            return null;
        }

        @Override
        protected OperationHandler<? extends AddElements> getAddElementsHandler() {
            return null;
        }

        @Override
        protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
            return ToBytesSerialiser.class;
        }
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