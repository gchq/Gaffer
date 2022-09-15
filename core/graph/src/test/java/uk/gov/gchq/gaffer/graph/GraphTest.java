/*
 * Copyright 2016-2021 Crown Copyright
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
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.GlobalViewElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.hook.AddOperationsToChain;
import uk.gov.gchq.gaffer.graph.hook.FunctionAuthoriser;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.gaffer.graph.hook.Log4jLogger;
import uk.gov.gchq.gaffer.graph.hook.NamedOperationResolver;
import uk.gov.gchq.gaffer.graph.hook.NamedViewResolver;
import uk.gov.gchq.gaffer.graph.hook.OperationAuthoriser;
import uk.gov.gchq.gaffer.graph.hook.OperationChainLimiter;
import uk.gov.gchq.gaffer.graph.hook.UpdateViewHook;
import uk.gov.gchq.gaffer.integration.store.TestStore;
import uk.gov.gchq.gaffer.jobtracker.Job;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jobtracker.Repeat;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedDoubleSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.handler.GetTraitsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.function.CreateObject;
import uk.gov.gchq.koryphe.impl.function.Identity;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;

@ExtendWith(MockitoExtension.class)
public class GraphTest {

    private static final String GRAPH_ID = "graphId";
    public static final String SCHEMA_ID_1 = "schemaId1";
    public static final String STORE_PROPERTIES_ID_1 = "storePropertiesId1";

    @TempDir
    Path tempDir;

    @Mock
    private User user;
    @Mock
    private Context context;
    @Mock
    private Context clonedContext;

    @Mock
    private OperationChain opChain;
    @Mock
    private OperationChain clonedOpChain;
    @Mock
    private GetElements operation;

    @BeforeEach
    public void before() throws Exception {
        HashMapGraphLibrary.clear();
        Mockito.reset(TestStore.mockStore);

        lenient().when(context.getUser()).thenReturn(user);
        lenient().when(context.shallowClone()).thenReturn(clonedContext);
        lenient().when(clonedContext.getUser()).thenReturn(user);

        lenient().when(opChain.getOperations()).thenReturn(Lists.newArrayList(operation));
        lenient().when(opChain.shallowClone()).thenReturn(clonedOpChain);
        lenient().when(clonedOpChain.getOperations()).thenReturn(Lists.newArrayList(operation));
    }

    @AfterEach
    public void after() {
        Mockito.reset(TestStore.mockStore);
        HashMapGraphLibrary.clear();
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
                        .directed(DIRECTED_EITHER)
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
                        .directed(DIRECTED_EITHER)
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
                .type(DIRECTED_EITHER, Boolean.class)
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
    void shouldConstructGraphFromSchemaFolderPath() throws IOException {
        // Given
        final Schema expectedSchema = new Schema.Builder()
                .json(StreamUtil.elementsSchema(getClass()), StreamUtil.typesSchema(getClass()))
                .build();

        Graph graph = null;
        File schemaDir = null;

        schemaDir = createSchemaDirectory();

        // When
        graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .addSchema(Paths.get(schemaDir.getPath()))
                .build();

        // Then
        JsonAssert.assertEquals(expectedSchema.toJson(true), graph.getSchema().toJson(true));
    }

    @Test
    void shouldConstructGraphFromSchemaURI() throws IOException, URISyntaxException {
        // Given
        final URI typeInputUri = getResourceUri(StreamUtil.TYPES_SCHEMA);
        final URI schemaInputUri = getResourceUri(StreamUtil.ELEMENTS_SCHEMA);
        final URI storeInputUri = getResourceUri(StreamUtil.STORE_PROPERTIES);
        final Schema expectedSchema = new Schema.Builder()
                .json(StreamUtil.elementsSchema(getClass()), StreamUtil.typesSchema(getClass()))
                .build();
        Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(storeInputUri)
                .addSchemas(typeInputUri, schemaInputUri)
                .build();

        // Then
        JsonAssert.assertEquals(expectedSchema.toJson(true), graph.getSchema().toJson(true));
    }

    private URI getResourceUri(String resource) throws URISyntaxException {
        resource = resource.replaceFirst(Pattern.quote("/"), "");
        final URI resourceURI = getClass().getClassLoader().getResource(resource).toURI();
        if (resourceURI == null) {
            fail("Test json file not found: " + resource);
        }
        return resourceURI;
    }

    @Test
    public void shouldCreateNewContextInstanceWhenExecuteOperation(@Mock final Store store)
            throws OperationException, IOException {
        // Given
        final Schema schema = new Schema();
        given(store.getSchema()).willReturn(schema);
        given(store.getProperties()).willReturn(new StoreProperties());

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(new Schema.Builder().build())
                .build();

        // When
        graph.execute(operation, context);

        // Then
        verify(store).execute(Mockito.any(Output.class), eq(clonedContext));
    }

    @Test
    public void shouldCreateNewContextInstanceWhenExecuteOutputOperation(@Mock final Store store)
            throws OperationException, IOException {
        // Given
        final Schema schema = new Schema();
        given(store.getSchema()).willReturn(schema);
        given(store.getProperties()).willReturn(new StoreProperties());

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(new Schema.Builder().build())
                .build();

        // When
        graph.execute(opChain, context);

        // Then
        verify(store).execute(clonedOpChain, clonedContext);
    }

    @Test
    public void shouldCreateNewContextInstanceWhenExecuteJob(@Mock final Store store)
            throws OperationException, IOException {
        // Given
        final Schema schema = new Schema();
        given(store.getSchema()).willReturn(schema);
        given(store.getProperties()).willReturn(new StoreProperties());

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(new Schema.Builder().build())
                .build();

        // When
        graph.executeJob(opChain, context);

        // Then
        verify(store).executeJob(clonedOpChain, clonedContext);
    }

    @Test
    public void shouldCloseAllOperationInputsWhenExceptionIsThrownWhenExecuted(@Mock final Store store,
            @Mock final RuntimeException exception)
            throws OperationException, IOException {
        // Given
        given(store.execute(clonedOpChain, clonedContext)).willThrow(exception);
        final Schema schema = new Schema();
        given(store.getSchema()).willReturn(schema);
        given(store.getProperties()).willReturn(new StoreProperties());

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(new FunctionAuthoriser()) // skips json serialisation in default hook
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(new Schema.Builder().build())
                .build();

        // When / Then
        try {
            graph.execute(opChain, context);
            fail("Exception expected");
        } catch (final Exception e) {
            assertSame(exception, e);
            verify(clonedOpChain).close();
        }
    }

    @Test
    public void shouldCloseAllOperationInputsWhenExceptionIsThrownWhenJobExecuted(@Mock final Store store,
            @Mock final RuntimeException exception)
            throws OperationException, IOException {
        // Given
        given(store.executeJob(clonedOpChain, clonedContext)).willThrow(exception);
        final Schema schema = new Schema();
        given(store.getSchema()).willReturn(schema);
        given(store.getProperties()).willReturn(new StoreProperties());

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(new FunctionAuthoriser()) // skips json serialisation in default hook
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(new Schema.Builder().build())
                .build();

        // When / Then
        try {
            graph.executeJob(opChain, context);
            fail("Exception expected");
        } catch (final Exception e) {
            assertSame(exception, e);
            verify(clonedOpChain).close();
        }
    }

    @Test
    public void shouldCallAllGraphHooksBeforeOperationChainExecuted(@Mock final Store store,
            @Mock final GraphHook hook1,
            @Mock final GraphHook hook2)
            throws OperationException {
        // Given
        final Schema schema = new Schema();
        given(store.getSchema()).willReturn(schema);
        given(store.getProperties()).willReturn(new StoreProperties());
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
        graph.execute(opChain, context);

        // Then
        final InOrder inOrder = inOrder(hook1, hook2, operation);
        inOrder.verify(hook1).preExecute(clonedOpChain, clonedContext);
        inOrder.verify(hook2).preExecute(clonedOpChain, clonedContext);
        inOrder.verify(operation).setView(Mockito.any(View.class));
        verify(context).setOriginalOpChain(opChain);
    }

    @Test
    public void shouldCallAllGraphHooksBeforeJobExecuted(@Mock final Store store,
            @Mock final GraphHook hook1,
            @Mock final GraphHook hook2)
            throws OperationException {
        // Given
        final Schema schema = new Schema();
        given(store.getSchema()).willReturn(schema);
        given(store.getProperties()).willReturn(new StoreProperties());
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
        graph.executeJob(opChain, context);

        // Then
        final InOrder inOrder = inOrder(hook1, hook2, operation);
        inOrder.verify(hook1).preExecute(clonedOpChain, clonedContext);
        inOrder.verify(hook2).preExecute(clonedOpChain, clonedContext);
        inOrder.verify(operation).setView(Mockito.any(View.class));
        verify(context).setOriginalOpChain(opChain);
    }

    @Test
    public void shouldCallAllGraphHooksAfterOperationExecuted(@Mock final Store store,
            @Mock final GraphHook hook1,
            @Mock final GraphHook hook2,
            @Mock final JobDetail result1,
            @Mock final JobDetail result2,
            @Mock final JobDetail result3)
            throws OperationException {
        // Given
        final Schema schema = new Schema();

        given(store.getSchema()).willReturn(schema);
        given(store.getProperties()).willReturn(new StoreProperties());
        given(hook1.postExecute(result1, clonedOpChain, clonedContext)).willReturn(result2);
        given(hook2.postExecute(result2, clonedOpChain, clonedContext)).willReturn(result3);

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
        final ArgumentCaptor<Context> contextCaptor1 = ArgumentCaptor.forClass(Context.class);
        given(store.execute(captor.capture(), contextCaptor1.capture())).willReturn(result1);

        // When
        final Object actualResult = graph.execute(opChain, context);

        // Then
        final InOrder inOrder = inOrder(hook1, hook2);
        inOrder.verify(hook1).postExecute(result1, captor.getValue(), clonedContext);
        inOrder.verify(hook2).postExecute(result2, captor.getValue(), clonedContext);
        final List<Operation> ops = captor.getValue().getOperations();
        assertThat(ops).hasSize(1);
        assertSame(operation, ops.get(0));
        assertSame(actualResult, result3);
        verify(context).setOriginalOpChain(opChain);
    }

    @Test
    public void shouldCallAllGraphHooksAfterOperationChainExecuted(@Mock final Store store,
            @Mock final GraphHook hook1,
            @Mock final GraphHook hook2,
            @Mock final JobDetail result1,
            @Mock final JobDetail result2,
            @Mock final JobDetail result3)
            throws OperationException {
        // Given
        final Schema schema = new Schema();
        given(store.getSchema()).willReturn(schema);
        given(store.getProperties()).willReturn(new StoreProperties());
        given(hook1.postExecute(result1, clonedOpChain, clonedContext)).willReturn(result2);
        given(hook2.postExecute(result2, clonedOpChain, clonedContext)).willReturn(result3);

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

        given(store.execute(clonedOpChain, clonedContext)).willReturn(result1);

        // When
        final Object actualResult = graph.execute(opChain, context);

        // Then
        final InOrder inOrder = inOrder(hook1, hook2);
        inOrder.verify(hook1).postExecute(result1, clonedOpChain, clonedContext);
        inOrder.verify(hook2).postExecute(result2, clonedOpChain, clonedContext);
        assertSame(actualResult, result3);
        verify(context).setOriginalOpChain(opChain);
    }

    @Test
    public void shouldCallAllGraphHooksAfterJobExecuted(@Mock final Store store,
            @Mock final GraphHook hook1,
            @Mock final GraphHook hook2,
            @Mock final JobDetail result1,
            @Mock final JobDetail result2,
            @Mock final JobDetail result3)
            throws OperationException {
        // Given
        final Schema schema = new Schema();
        given(store.getSchema()).willReturn(schema);
        given(store.getProperties()).willReturn(new StoreProperties());
        given(hook1.postExecute(result1, clonedOpChain, clonedContext)).willReturn(result2);
        given(hook2.postExecute(result2, clonedOpChain, clonedContext)).willReturn(result3);

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

        given(store.executeJob(clonedOpChain, clonedContext)).willReturn(result1);

        // When
        final JobDetail actualResult = graph.executeJob(opChain, context);

        // Then
        final InOrder inOrder = inOrder(hook1, hook2);
        inOrder.verify(hook1).postExecute(result1, clonedOpChain, clonedContext);
        inOrder.verify(hook2).postExecute(result2, clonedOpChain, clonedContext);
        assertSame(actualResult, result3);
        verify(context).setOriginalOpChain(opChain);
    }

    @Test
    public void shouldCallAllGraphHooksOnGraphHookPreExecuteFailure(@Mock final Store store,
            @Mock final GraphHook hook1,
            @Mock final GraphHook hook2)
            throws OperationException {
        // Given
        final Schema schema = new Schema();
        given(store.getSchema()).willReturn(schema);
        given(store.getProperties()).willReturn(new StoreProperties());
        final RuntimeException e = new RuntimeException("Hook2 failed in postExecute");
        doThrow(e).when(hook1).preExecute(clonedOpChain, clonedContext);
        given(hook1.onFailure(null, clonedOpChain, clonedContext, e))
                .willThrow(new RuntimeException("Hook1 failed in onFailure"));
        given(hook2.onFailure(null, clonedOpChain, clonedContext, e)).willReturn(null);

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
            graph.execute(opChain, context);
            fail("Exception expected");
        } catch (final RuntimeException runtimeE) {
            final InOrder inOrder = inOrder(context, hook1, hook2);
            inOrder.verify(context).setOriginalOpChain(opChain);
            inOrder.verify(hook2, never()).preExecute(any(), any());
            inOrder.verify(hook1, never()).postExecute(any(), any(), any());
            inOrder.verify(hook2, never()).postExecute(any(), any(), any());
            inOrder.verify(hook1).onFailure(eq(null), any(), eq(clonedContext), eq(e));
            inOrder.verify(hook2).onFailure(eq(null), any(), eq(clonedContext), eq(e));
        }
    }

    @Test
    public void shouldCallAllGraphHooksOnGraphHookPostExecuteFailure(@Mock final Store store,
            @Mock final GraphHook hook1,
            @Mock final GraphHook hook2,
            @Mock final JobDetail result1,
            @Mock final JobDetail result2,
            @Mock final JobDetail result3)
            throws OperationException {
        // Given
        final Schema schema = new Schema();
        given(store.getSchema()).willReturn(schema);
        given(store.getProperties()).willReturn(new StoreProperties());
        given(hook1.postExecute(result1, clonedOpChain, clonedContext)).willReturn(result2);
        final RuntimeException e = new RuntimeException("Hook2 failed in postExecute");
        given(hook2.postExecute(result2, clonedOpChain, clonedContext)).willThrow(e);
        given(hook1.onFailure(result2, clonedOpChain, clonedContext, e))
                .willThrow(new RuntimeException("Hook1 failed in onFailure"));
        given(hook2.onFailure(result2, clonedOpChain, clonedContext, e)).willReturn(result3);

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
        given(store.execute(captor.capture(), eq(clonedContext))).willReturn(result1);

        // When / Then
        try {
            graph.execute(opChain, context);
            fail("Exception expected");
        } catch (final RuntimeException runtimeE) {
            final InOrder inOrder = inOrder(context, hook1, hook2);
            inOrder.verify(context).setOriginalOpChain(opChain);
            inOrder.verify(hook1).postExecute(result1, captor.getValue(), clonedContext);
            inOrder.verify(hook2).postExecute(result2, captor.getValue(), clonedContext);
            inOrder.verify(hook1).onFailure(result2, captor.getValue(), clonedContext, e);
            inOrder.verify(hook2).onFailure(result2, captor.getValue(), clonedContext, e);
            final List<Operation> ops = captor.getValue().getOperations();
            assertThat(ops).hasSize(1);
            assertSame(operation, ops.get(0));
        }
    }

    @Test
    public void shouldCallAllGraphHooksOnExecuteFailure(@Mock final Store store,
            @Mock final GraphHook hook1,
            @Mock final GraphHook hook2)
            throws OperationException {
        // Given
        final Schema schema = new Schema();
        given(store.getSchema()).willReturn(schema);
        given(store.getProperties()).willReturn(new StoreProperties());

        final RuntimeException e = new RuntimeException("Store failed to execute operation chain");
        given(hook1.onFailure(null, clonedOpChain, clonedContext, e))
                .willThrow(new RuntimeException("Hook1 failed in onFailure"));
        given(hook2.onFailure(null, clonedOpChain, clonedContext, e)).willReturn(null);

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
        given(store.execute(captor.capture(), eq(clonedContext))).willThrow(e);

        // When / Then
        try {
            graph.execute(opChain, context);
            fail("Exception expected");
        } catch (final RuntimeException runtimeE) {
            final InOrder inOrder = inOrder(context, clonedContext, hook1, hook2);
            inOrder.verify(context).setOriginalOpChain(opChain);
            inOrder.verify(hook1, never()).postExecute(any(), any(), any());
            inOrder.verify(hook2, never()).postExecute(any(), any(), any());
            inOrder.verify(hook1).onFailure(null, captor.getValue(), clonedContext, e);
            inOrder.verify(hook2).onFailure(null, captor.getValue(), clonedContext, e);
            final List<Operation> ops = captor.getValue().getOperations();
            assertThat(ops).hasSize(1);
            assertSame(operation, ops.get(0));
        }
    }

    @Test
    public void shouldCallAllGraphHooksOnGraphHookPreExecuteFailureWhenRunningJob(@Mock final Store store,
            @Mock final GraphHook hook1,
            @Mock final GraphHook hook2)
            throws OperationException {
        // Given
        final Schema schema = new Schema();
        given(store.getSchema()).willReturn(schema);
        given(store.getProperties()).willReturn(new StoreProperties());
        final RuntimeException e = new RuntimeException("Hook2 failed in postExecute");
        doThrow(e).when(hook1).preExecute(clonedOpChain, clonedContext);
        given(hook1.onFailure(null, clonedOpChain, clonedContext, e))
                .willThrow(new RuntimeException("Hook1 failed in onFailure"));
        given(hook2.onFailure(null, clonedOpChain, clonedContext, e)).willReturn(null);

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
            graph.executeJob(opChain, context);
            fail("Exception expected");
        } catch (final RuntimeException runtimeE) {
            final InOrder inOrder = inOrder(context, hook1, hook2);
            inOrder.verify(context).setOriginalOpChain(opChain);
            inOrder.verify(hook2, never()).preExecute(any(), any());
            inOrder.verify(hook1, never()).postExecute(any(), any(), any());
            inOrder.verify(hook2, never()).postExecute(any(), any(), any());
            inOrder.verify(hook1).onFailure(eq(null), any(), eq(clonedContext), eq(e));
            inOrder.verify(hook2).onFailure(eq(null), any(), eq(clonedContext), eq(e));
        }
    }

    @Test
    public void shouldCallAllGraphHooksOnGraphHookPostExecuteFailureWhenRunningJob(@Mock final Store store,
            @Mock final GraphHook hook1,
            @Mock final GraphHook hook2,
            @Mock final JobDetail result1,
            @Mock final JobDetail result2,
            @Mock final JobDetail result3)
            throws OperationException {
        // Given
        final Schema schema = new Schema();

        given(store.getSchema()).willReturn(schema);
        given(store.getProperties()).willReturn(new StoreProperties());
        given(hook1.postExecute(result1, clonedOpChain, clonedContext)).willReturn(result2);
        final RuntimeException e = new RuntimeException("Hook2 failed in postExecute");
        given(hook2.postExecute(result2, clonedOpChain, clonedContext)).willThrow(e);
        given(hook1.onFailure(result2, clonedOpChain, clonedContext, e))
                .willThrow(new RuntimeException("Hook1 failed in onFailure"));
        given(hook2.onFailure(result2, clonedOpChain, clonedContext, e)).willReturn(result3);

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
        given(store.executeJob(captor.capture(), eq(clonedContext))).willReturn(result1);

        // When / Then
        try {
            graph.executeJob(opChain, context);
            fail("Exception expected");
        } catch (final RuntimeException runtimeE) {
            final InOrder inOrder = inOrder(context, hook1, hook2);
            inOrder.verify(context).setOriginalOpChain(opChain);
            inOrder.verify(hook1).postExecute(result1, captor.getValue(), clonedContext);
            inOrder.verify(hook2).postExecute(result2, captor.getValue(), clonedContext);
            inOrder.verify(hook1).onFailure(result2, captor.getValue(), clonedContext, e);
            inOrder.verify(hook2).onFailure(result2, captor.getValue(), clonedContext, e);
            final List<Operation> ops = captor.getValue().getOperations();
            assertThat(ops).hasSize(1);
            assertSame(operation, ops.get(0));
        }
    }

    @Test
    public void shouldCallAllGraphHooksOnExecuteFailureWhenRunningJob(@Mock final Store store,
            @Mock final GraphHook hook1,
            @Mock final GraphHook hook2,
            @Mock final Operation operation,
            @Mock final OperationChain<Integer> opChain,
            @Mock final OperationChain<Integer> clonedOpChain)
            throws OperationException {
        // Given
        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));

        final Schema schema = new Schema();
        final RuntimeException e = new RuntimeException("Store failed to execute operation chain");

        given(store.getSchema()).willReturn(schema);
        given(store.getProperties()).willReturn(new StoreProperties());
        given(hook1.onFailure(null, clonedOpChain, clonedContext, e))
                .willThrow(new RuntimeException("Hook1 failed in onFailure"));
        given(hook2.onFailure(null, clonedOpChain, clonedContext, e)).willReturn(null);

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
        given(store.executeJob(captor.capture(), eq(clonedContext))).willThrow(e);

        // When / Then
        try {
            graph.executeJob(opChain, context);
            fail("Exception expected");
        } catch (final RuntimeException runtimeE) {
            final InOrder inOrder = inOrder(context, hook1, hook2);
            inOrder.verify(context).setOriginalOpChain(opChain);
            inOrder.verify(hook1, never()).postExecute(any(), any(), any());
            inOrder.verify(hook2, never()).postExecute(any(), any(), any());
            inOrder.verify(hook1).onFailure(null, captor.getValue(), clonedContext, e);
            inOrder.verify(hook2).onFailure(null, captor.getValue(), clonedContext, e);
            final List<Operation> ops = captor.getValue().getOperations();
            assertThat(ops).hasSize(1);
            assertSame(operation, ops.get(0));
        }
    }

    @Test
    public void shouldConstructGraphAndCreateViewWithGroups(@Mock final Store store) {
        // Given
        given(store.getGraphId()).willReturn(GRAPH_ID);
        given(store.getProperties()).willReturn(new StoreProperties());

        final Map<String, SchemaEdgeDefinition> edges = new HashMap<>();
        edges.put("edge1", new SchemaEdgeDefinition());
        edges.put("edge2", new SchemaEdgeDefinition());
        edges.put("edge3", new SchemaEdgeDefinition());
        edges.put("edge4", new SchemaEdgeDefinition());

        final Map<String, SchemaEntityDefinition> entities = new HashMap<>();
        entities.put("entity1", new SchemaEntityDefinition());
        entities.put("entity2", new SchemaEntityDefinition());
        entities.put("entity3", new SchemaEntityDefinition());
        entities.put("entity4", new SchemaEntityDefinition());

        final Schema schema = new Schema.Builder().edges(edges).entities(entities).build();
        given(store.getSchema()).willReturn(schema);

        // When
        final View resultView = new Graph.Builder()
                .store(store)
                .build()
                .getView();

        // Then
        assertNotSame(schema, resultView);
        assertArrayEquals(entities.keySet().toArray(), resultView.getEntityGroups().toArray());
        assertArrayEquals(edges.keySet().toArray(), resultView.getEdgeGroups().toArray());

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
    public void shouldExposeGetTraitsMethod(@Mock final Store store,
            @Mock final View view)
            throws OperationException {
        // Given
        given(store.getSchema()).willReturn(new Schema());
        given(store.getProperties()).willReturn(new StoreProperties());
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .view(view)
                        .build())
                .store(store)
                .build();

        // When
        final Set<StoreTrait> storeTraits = new HashSet<>(
                Arrays.asList(StoreTrait.INGEST_AGGREGATION, StoreTrait.TRANSFORMATION));
        given(store.getTraits()).willReturn(storeTraits);
        final Collection<StoreTrait> returnedTraits = graph.getStoreTraits();

        // Then
        assertEquals(returnedTraits, storeTraits);

    }

    @Test
    public void shouldGetSchemaFromStoreIfSchemaIsEmpty(@Mock final Store store,
            @Mock final View view)
            throws OperationException {
        // Given
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .build())
                .type("string", String.class)
                .build();
        given(store.getSchema()).willReturn(schema);
        given(store.getProperties()).willReturn(new StoreProperties());
        new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .view(view)
                        .build())
                .addSchema(new Schema())
                .store(store)
                .build();

        // When
        verify(store).setOriginalSchema(schema);
    }

    @Test
    public void shouldSetGraphViewOnOperationAndDelegateDoOperationToStore(@Mock final Store store,
            @Mock final OperationChain<Integer> opChain,
            @Mock final OperationChain<Integer> clonedOpChain)
            throws OperationException {
        // Given
        given(store.getSchema()).willReturn(new Schema());
        given(store.getProperties()).willReturn(new StoreProperties());
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY)
                .edge(TestGroups.EDGE)
                .build();
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .view(view)
                        .build())
                .store(store)
                .build();
        final Integer expectedResult = 5;
        final GetElements operation = new GetElements();

        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));
        given(store.execute(clonedOpChain, clonedContext)).willReturn(expectedResult);

        // When
        final Integer result = graph.execute(opChain, context);

        // Then
        assertEquals(expectedResult, result);
        verify(store).execute(clonedOpChain, clonedContext);
        JsonAssert.assertEquals(view.toJson(false), operation.getView().toJson(false));
    }

    @Test
    public void shouldNotSetGraphViewOnOperationWhenOperationViewIsNotNull(@Mock final Store store,
            @Mock final View view,
            @Mock final View opView,
            @Mock final OperationChain<Integer> opChain,
            @Mock final OperationChain<Integer> clonedOpChain)
            throws OperationException {
        // Given
        given(store.getSchema()).willReturn(new Schema());
        given(store.getProperties()).willReturn(new StoreProperties());
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .view(view)
                        .build())
                .store(store)
                .build();
        final Integer expectedResult = 5;

        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(store.execute(clonedOpChain, clonedContext)).willReturn(expectedResult);

        // When
        final Integer result = graph.execute(opChain, context);

        // Then
        assertEquals(expectedResult, result);
        verify(store).execute(clonedOpChain, clonedContext);
        verify(operation, Mockito.never()).setView(view);
    }

    @Test
    public void shouldNotSetGraphViewOnOperationWhenOperationIsNotAGet(@Mock final Store store,
            @Mock final View view,
            @Mock final Operation operation,
            @Mock final OperationChain<Integer> opChain,
            @Mock final OperationChain<Integer> clonedOpChain)
            throws OperationException {
        // Given
        given(store.getSchema()).willReturn(new Schema());
        given(store.getProperties()).willReturn(new StoreProperties());
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .view(view)
                        .build())
                .store(store)
                .build();
        final int expectedResult = 5;

        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));
        given(store.execute(clonedOpChain, clonedContext)).willReturn(expectedResult);

        // When
        final int result = graph.execute(opChain, context);

        // Then
        assertEquals(expectedResult, result);
        verify(store).execute(clonedOpChain, clonedContext);
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
            assertEquals("The Store class name was not found in the store properties for key: "
                    + StoreProperties.STORE_CLASS + ", GraphId: " + GRAPH_ID, e.getMessage());
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
                                    .serialiser(new OrderedDoubleSerialiser())
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
    public void shouldDelegateGetNextOperationsToStore(@Mock final Store store,
            @Mock final Set<Class<? extends Operation>> expectedNextOperations) {
        // Given
        given(store.getSchema()).willReturn(new Schema());
        given(store.getProperties()).willReturn(new StoreProperties());
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .store(store)
                .build();

        given(store.getNextOperations(GetElements.class)).willReturn(expectedNextOperations);

        // When
        final Set<Class<? extends Operation>> nextOperations = graph.getNextOperations(GetElements.class);

        // Then
        assertSame(expectedNextOperations, nextOperations);
    }

    @Test
    public void shouldDelegateIsSupportedToStore(@Mock final Store store) {
        // Given
        given(store.getSchema()).willReturn(new Schema());
        given(store.getProperties()).willReturn(new StoreProperties());
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
    public void shouldDelegateGetSupportedOperationsToStore(@Mock final Store store,
            @Mock final Set<Class<? extends Operation>> expectedSupportedOperations) {
        // Given
        given(store.getSchema()).willReturn(new Schema());
        given(store.getProperties()).willReturn(new StoreProperties());
        given(store.getSupportedOperations()).willReturn(expectedSupportedOperations);

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .store(store)
                .build();

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

        // When / Then
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

        // When / Then
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
        writeToFile("elements.json", tempDir.toFile());
        writeToFile("types.json", tempDir.toFile());
        return tempDir.toFile();
    }

    private void writeToFile(final String schemaFile, final File dir) throws IOException {
        Files.copy(new File(getClass().getResource("/schema/" + schemaFile).getPath()),
                new File(dir + "/" + schemaFile));
    }

    @Test
    public void shouldThrowExceptionIfGraphIdIsInvalid(@Mock final StoreProperties properties) throws Exception {
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
                        .directed(DIRECTED_EITHER)
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
                        .directed(DIRECTED_EITHER)
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
                .type(DIRECTED_EITHER, Boolean.class)
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
    public void shouldAddHooksVarArgsAndGetGraphHooks(@Mock final GraphHook graphHook1,
            @Mock final Log4jLogger graphHook2)
            throws Exception {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStoreImpl.class.getName());

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
        assertEquals(Arrays.asList(NamedViewResolver.class, graphHook1.getClass(), graphHook2.getClass(),
                FunctionAuthoriser.class), graph.getGraphHooks());
    }

    @Test
    public void shouldAddHookAndGetGraphHooks(@Mock final GraphHook graphHook1,
            @Mock final Log4jLogger graphHook3)
            throws Exception {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStore.class.getName());
        given(TestStore.mockStore.isSupported(NamedOperation.class)).willReturn(true);
        final NamedOperationResolver graphHook2 = new NamedOperationResolver();

        // When
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .addHook(graphHook1)
                        .addHook(graphHook2)
                        .addHook(graphHook3)
                        .build())
                .storeProperties(storeProperties)
                .addSchemas(StreamUtil.schemas(getClass()))
                .build();

        // Then
        assertEquals(Arrays.asList(NamedViewResolver.class, graphHook1.getClass(), graphHook2.getClass(),
                graphHook3.getClass(), FunctionAuthoriser.class), graph.getGraphHooks());
    }

    @Test
    public void shouldAddNamedViewResolverHookAfterNamedOperationResolver(@Mock final GraphHook graphHook1,
            @Mock final Log4jLogger graphHook2)
            throws Exception {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStore.class.getName());
        given(TestStore.mockStore.isSupported(NamedOperation.class)).willReturn(true);

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
        assertEquals(Arrays.asList(NamedOperationResolver.class, NamedViewResolver.class, graphHook1.getClass(),
                graphHook2.getClass(), FunctionAuthoriser.class), graph.getGraphHooks());
    }

    @Test
    public void shouldAddHooksFromPathAndGetGraphHooks() throws Exception {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStoreImpl.class.getName());

        final File graphHooks = tempDir.resolve("graphHooks.json").toFile();
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
                Arrays.asList(NamedViewResolver.class, OperationChainLimiter.class, AddOperationsToChain.class,
                        OperationAuthoriser.class, FunctionAuthoriser.class),
                graph.getGraphHooks());
    }

    @Test
    public void shouldAddHookFromPathAndGetGraphHooks() throws Exception {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStoreImpl.class.getName());

        final File graphHook1File = tempDir.resolve("opChainLimiter.json").toFile();
        FileUtils.writeLines(graphHook1File,
                IOUtils.readLines(StreamUtil.openStream(getClass(), "opChainLimiter.json")));

        final File graphHook2File = tempDir.resolve("opAuthoriser.json").toFile();
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
        assertEquals(Arrays.asList(NamedViewResolver.class, OperationChainLimiter.class, OperationAuthoriser.class,
                FunctionAuthoriser.class), graph.getGraphHooks());
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
        assertEquals(Arrays.asList(NamedViewResolver.class, OperationChainLimiter.class, AddOperationsToChain.class,
                FunctionAuthoriser.class), graph.getGraphHooks());
    }

    @Test
    public void shouldBuildGraphFromConfigAndMergeConfigWithExistingConfig(
            @Mock final GraphLibrary library1, @Mock final GraphLibrary library2,
            @Mock final GraphHook hook1, @Mock final GraphHook hook2, @Mock final GraphHook hook3)
            throws Exception {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStoreImpl.class.getName());

        final String graphId1 = "graphId1";
        final String graphId2 = "graphId2";

        final View view1 = new View.Builder().entity(TestGroups.ENTITY).build();
        final View view2 = new View.Builder().edge(TestGroups.EDGE).build();

        // When
        final GraphConfig config = new GraphConfig.Builder()
                .graphId(graphId2)
                .library(library2)
                .addHook(hook2)
                .view(view2)
                .build();

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(graphId1)
                        .library(library1)
                        .view(view1)
                        .addHook(hook1)
                        .build())
                .storeProperties(storeProperties)
                .addSchemas(StreamUtil.schemas(getClass()))
                .config(config)
                .config(new GraphConfig.Builder()
                        .addHook(hook3)
                        .build())
                .build();

        // Then
        assertEquals(graphId2, graph.getGraphId());
        assertEquals(view2, graph.getView());
        assertEquals(library2, graph.getGraphLibrary());
        assertEquals(
                Arrays.asList(NamedViewResolver.class, hook1.getClass(), hook2.getClass(), hook3.getClass(),
                        FunctionAuthoriser.class),
                graph.getGraphHooks());
    }

    @Test
    public void shouldBuildGraphFromConfigAndOverrideFields(
            @Mock final GraphLibrary library1, @Mock final GraphLibrary library2,
            @Mock final GraphHook hook1, @Mock final GraphHook hook2, @Mock final GraphHook hook3)
            throws Exception {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStoreImpl.class.getName());

        final String graphId1 = "graphId1";
        final String graphId2 = "graphId2";

        final View view1 = new View.Builder().entity(TestGroups.ENTITY).build();
        final View view2 = new View.Builder().edge(TestGroups.EDGE).build();

        // When
        final GraphConfig config = new GraphConfig.Builder()
                .graphId(graphId2)
                .library(library2)
                .addHook(hook2)
                .view(view2)
                .build();

        final Graph graph = new Graph.Builder()
                .config(config)
                .config(new GraphConfig.Builder()
                        .graphId(graphId1)
                        .library(library1)
                        .view(view1)
                        .build())
                .storeProperties(storeProperties)
                .addSchemas(StreamUtil.schemas(getClass()))
                .config(new GraphConfig.Builder()
                        .addHook(hook1)
                        .build())
                .config(new GraphConfig.Builder()
                        .addHook(hook3)
                        .build())
                .build();

        // Then
        assertEquals(graphId1, graph.getGraphId());
        assertEquals(view1, graph.getView());
        assertEquals(library1, graph.getGraphLibrary());
        assertEquals(
                Arrays.asList(NamedViewResolver.class, hook2.getClass(), hook1.getClass(), hook3.getClass(),
                        FunctionAuthoriser.class),
                graph.getGraphHooks());
    }

    @Test
    public void shouldReturnClonedViewFromConfig() throws Exception {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStoreImpl.class.getName());

        final String graphId = "graphId";

        final View view = new View.Builder().entity(TestGroups.ENTITY).build();

        // When
        final GraphConfig config = new GraphConfig.Builder()
                .graphId(graphId)
                .view(view)
                .build();

        final Graph graph = new Graph.Builder()
                .config(config)
                .storeProperties(storeProperties)
                .addSchemas(StreamUtil.schemas(getClass()))
                .build();

        // Then
        assertEquals(graphId, graph.getGraphId());
        assertEquals(view, graph.getView());
        assertNotSame(view, graph.getView());
    }

    @Test
    public void shouldBuildGraphFromConfigAndSetIdsToGraphsWhenDifferent() {
        // Given
        final StoreProperties libraryStoreProperties = new StoreProperties();
        libraryStoreProperties.setStoreClass(TestStoreImpl.class.getName());

        final StoreProperties graphStoreProperties = new StoreProperties();
        graphStoreProperties.setStoreClass(TestStoreImpl.class.getName());

        final Schema librarySchema = new Schema.Builder().build();

        final Schema graphSchema = new Schema.Builder().build();

        final String graphId1 = "graphId1";

        final HashMapGraphLibrary library = new HashMapGraphLibrary();
        library.addSchema(SCHEMA_ID_1, librarySchema);
        library.addProperties(STORE_PROPERTIES_ID_1, libraryStoreProperties);

        // When
        final GraphConfig config = new GraphConfig.Builder()
                .graphId(graphId1)
                .library(library)
                .build();

        final Graph graph1 = new Graph.Builder()
                .config(config)
                .addToLibrary(true)
                .parentStorePropertiesId("storePropertiesId1")
                .storeProperties(graphStoreProperties)
                .addParentSchemaIds(SCHEMA_ID_1)
                .addSchemas(graphSchema)
                .build();

        // Then
        assertEquals(graphId1, graph1.getGraphId());
        JsonAssert.assertEquals(library.getSchema(SCHEMA_ID_1).toJson(false), librarySchema.toJson(false));
        final Pair<String, String> ids = library.getIds(graphId1);
        // Check that the schemaIds are different between the parent and supplied schema
        assertEquals(graphId1, ids.getFirst());
        // Check that the storePropsIds are different between the parent and supplied
        // storeProps
        assertEquals(graphId1, ids.getSecond());
    }

    @Test
    public void shouldBuildGraphFromConfigAndSetIdsToGraphsWhenIdentical() {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStoreImpl.class.getName());
        final String storePropertiesId1 = "storePropertiesId1";

        final Schema schema = new Schema.Builder().build();

        final String graphId1 = "graphId1";

        final HashMapGraphLibrary library = new HashMapGraphLibrary();
        library.addSchema(SCHEMA_ID_1, schema);
        library.addProperties(storePropertiesId1, storeProperties);

        // When
        final GraphConfig config = new GraphConfig.Builder()
                .graphId(graphId1)
                .library(library)
                .build();

        final Graph graph1 = new Graph.Builder()
                .config(config)
                .addToLibrary(true)
                .parentStorePropertiesId(storePropertiesId1)
                .storeProperties(storeProperties)
                .addParentSchemaIds(SCHEMA_ID_1)
                .addSchemas(schema)
                .build();

        // Then
        assertEquals(graphId1, graph1.getGraphId());
        JsonAssert.assertEquals(library.getSchema(SCHEMA_ID_1).toJson(false), schema.toJson(false));
        // Check that the schemaId = schemaId1 as both the parent and supplied schema
        // have same id's
        // Check that the storePropsId = storePropertiesId1 as both parent and supplied
        // storeProps have same id's
        assertThat(graphId1)
                .isEqualTo(library.getIds(graphId1).getFirst())
                .isEqualTo(library.getIds(graphId1).getSecond());

    }

    @Test
    public void shouldBuildGraphFromConfigAndSetIdsToGraphsWhenGraphSchemaAndStorePropertiesIdsAreNull() {
        // Given
        final StoreProperties libraryStoreProperties = new StoreProperties();
        libraryStoreProperties.setStoreClass(TestStoreImpl.class.getName());

        final StoreProperties graphStoreProperties = new StoreProperties();
        graphStoreProperties.setStoreClass(TestStoreImpl.class.getName());

        final Schema librarySchema = new Schema.Builder().build();

        final Schema graphSchema = new Schema.Builder().build();

        final String graphId1 = "graphId1";

        final HashMapGraphLibrary library = new HashMapGraphLibrary();
        library.addSchema(SCHEMA_ID_1, librarySchema);
        library.addProperties(STORE_PROPERTIES_ID_1, libraryStoreProperties);

        // When
        final GraphConfig config = new GraphConfig.Builder()
                .graphId(graphId1)
                .library(library)
                .build();

        final Graph graph1 = new Graph.Builder()
                .config(config)
                .addToLibrary(true)
                .parentStorePropertiesId("storePropertiesId1")
                .storeProperties(graphStoreProperties)
                .addParentSchemaIds(SCHEMA_ID_1)
                .addSchemas(graphSchema)
                .build();

        // Then
        assertEquals(graphId1, graph1.getGraphId());
        JsonAssert.assertEquals(library.getSchema(SCHEMA_ID_1).toJson(false), librarySchema.toJson(false));
        // Check that the schemaId = schemaId1 as both the supplied schema id is null
        // Check that the storePropsId = storePropertiesId1 as the supplied storeProps
        // id is null
        assertThat(graphId1)
                .isEqualTo(library.getIds(graphId1).getFirst())
                .isEqualTo(library.getIds(graphId1).getSecond());
    }

    @Test
    public void shouldCorrectlySetViewForNestedOperationChain() throws OperationException {
        // Given
        final Store store = new TestStore();
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(new StoreProperties())
                .addSchema(new Schema.Builder()
                        .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                                .property(TestPropertyNames.PROP_1, TestTypes.PROP_INTEGER)
                                .aggregate(false)
                                .source("vertex2")
                                .destination("vertex2")
                                .directed(DIRECTED_EITHER)
                                .build())
                        .type(TestTypes.PROP_INTEGER, new TypeDefinition.Builder()
                                .clazz(Integer.class)
                                .build())
                        .type("vertex2", new TypeDefinition.Builder()
                                .clazz(String.class)
                                .build())
                        .type(DIRECTED_EITHER, Boolean.class)
                        .build())
                .store(store).build();
        final User user = new User();
        final Context context = new Context(user);

        final OperationChain<Iterable<? extends Element>> nestedChain = new OperationChain<>(
                Arrays.asList(
                        new GetAllElements(),
                        new Limit<>(3, true)));
        final OperationChain<Iterable<? extends Element>> outerChain = new OperationChain<>(nestedChain);

        graph.execute(outerChain, context);

        // Then
        assertNotNull(graph.getView());
    }

    @Test
    public void shouldThrowExceptionOnExecuteWithANullContext(@Mock final OperationChain opChain)
            throws OperationException {
        // Given
        final Context context = null;

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .addSchemas(StreamUtil.schemas(getClass()))
                .build();

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graph.execute(opChain, context))
                .withMessage("A context containing a user is required");
    }

    @Test
    public void shouldThrowExceptionOnExecuteJobWithANullContext(@Mock final OperationChain opChain)
            throws OperationException {
        // Given
        final Context context = null;

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .addSchemas(StreamUtil.schemas(getClass()))
                .build();

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graph.executeJob(opChain, context))
                .withMessage("A context containing a user is required");
    }

    @Test
    public void shouldThrowExceptionOnExecuteWithANullUser(@Mock final OperationChain opChain)
            throws OperationException {
        // Given
        final User user = null;

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .addSchemas(StreamUtil.schemas(getClass()))
                .build();

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graph.execute(opChain, user))
                .withMessage("A user is required");
    }

    @Test
    public void shouldThrowExceptionOnExecuteJobWithANullUser(@Mock final OperationChain opChain)
            throws OperationException {
        // Given
        final User user = null;

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .addSchemas(StreamUtil.schemas(getClass()))
                .build();

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graph.executeJob(opChain, user))
                .withMessage("A user is required");
    }

    @Test
    public void shouldThrowExceptionOnExecuteJobUsingJobWithANullContext(@Mock final OperationChain opChain)
            throws OperationException {
        // Given
        final Context context = null;

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .addSchemas(StreamUtil.schemas(getClass()))
                .build();

        final Job job = new Job(null, opChain);

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graph.executeJob(job, context))
                .withMessage("A context is required");
    }

    @Test
    public void shouldThrowExceptionOnExecuteJobUsingJobWithANullOperation() throws OperationException {
        // Given
        final Context context = new Context();

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .addSchemas(StreamUtil.schemas(getClass()))
                .build();

        final Job job = new Job(new Repeat(), new OperationChain<>());

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graph.executeJob(job, context))
                .withMessage("An operation is required");
    }

    @Test
    public void shouldThrowExceptionOnExecuteJobUsingJobWithANullJob() throws OperationException {
        // Given
        final Context context = new Context();

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .addSchemas(StreamUtil.schemas(getClass()))
                .build();

        final Job job = null;

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graph.executeJob(job, context))
                .withMessage("A job is required");
    }

    @Test
    public void shouldThrowExceptionOnExecuteJobUsingJobWithANullUser(@Mock final OperationChain opChain)
            throws OperationException {
        // Given
        final User user = null;

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .addSchemas(StreamUtil.schemas(getClass()))
                .build();

        final Job job = new Job(null, opChain);

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graph.executeJob(job, user))
                .withMessage("User is required");
    }

    @Test
    public void shouldManipulateViewRemovingBlacklistedEdgeUsingUpdateViewHook(@Mock final Store store)
            throws OperationException {
        // Given
        operation = new GetElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE_5)
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        final UpdateViewHook updateViewHook = new UpdateViewHook.Builder()
                .blackListElementGroups(Collections.singleton(TestGroups.EDGE))
                .build();

        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));
        given(clonedOpChain.flatten()).willReturn(Arrays.asList(operation));

        given(store.getSchema()).willReturn(new Schema());
        given(store.getProperties()).willReturn(new StoreProperties());

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(updateViewHook)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .build();

        final ArgumentCaptor<OperationChain> captor = ArgumentCaptor.forClass(OperationChain.class);
        final ArgumentCaptor<Context> contextCaptor1 = ArgumentCaptor.forClass(Context.class);

        given(store.execute(captor.capture(), contextCaptor1.capture())).willReturn(new ArrayList<>());

        // When / Then
        graph.execute(opChain, user);

        final List<Operation> ops = captor.getValue().getOperations();

        JsonAssert.assertEquals(new View.Builder().edge(TestGroups.EDGE_5).build().toCompactJson(),
                ((GetElements) ops.get(0)).getView().toCompactJson());
    }

    @Test
    public void shouldManipulateViewRemovingBlacklistedEdgeLeavingEmptyViewUsingUpdateViewHook(@Mock final Store store)
            throws OperationException {
        // Given
        operation = new GetElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        final UpdateViewHook updateViewHook = new UpdateViewHook.Builder()
                .blackListElementGroups(Collections.singleton(TestGroups.EDGE))
                .build();

        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));
        given(clonedOpChain.flatten()).willReturn(Arrays.asList(operation));

        given(store.getSchema()).willReturn(new Schema());
        given(store.getProperties()).willReturn(new StoreProperties());

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(updateViewHook)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .build();

        final ArgumentCaptor<OperationChain> captor = ArgumentCaptor.forClass(OperationChain.class);
        final ArgumentCaptor<Context> contextCaptor1 = ArgumentCaptor.forClass(Context.class);

        given(store.execute(captor.capture(), contextCaptor1.capture())).willReturn(new ArrayList<>());

        // When / Then
        graph.execute(opChain, user);

        final List<Operation> ops = captor.getValue().getOperations();

        JsonAssert.assertEquals(new View.Builder().build().toCompactJson(),
                ((GetElements) ops.get(0)).getView().toCompactJson());
    }

    @Test
    public void shouldRerunMultipleUpdateViewHooksToRemoveAllBlacklistedElements(@Mock final Store store)
            throws OperationException {
        // Given
        operation = new GetElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .edge(TestGroups.EDGE_2)
                        .build())
                .build();

        final UpdateViewHook first = new UpdateViewHook.Builder()
                .blackListElementGroups(Collections.singleton(TestGroups.EDGE))
                .withOpAuth(Sets.newHashSet("opAuth1"))
                .build();

        final UpdateViewHook second = new UpdateViewHook.Builder()
                .blackListElementGroups(Collections.singleton(TestGroups.EDGE_2))
                .withOpAuth(Sets.newHashSet("opAuth2"))
                .build();

        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));
        given(clonedOpChain.flatten()).willReturn(Arrays.asList(operation));

        given(store.getSchema()).willReturn(new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition())
                .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition())
                .build());
        given(store.getProperties()).willReturn(new StoreProperties());

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(first)
                        .addHook(second)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .build();

        final ArgumentCaptor<OperationChain> captor = ArgumentCaptor.forClass(OperationChain.class);
        final ArgumentCaptor<Context> contextCaptor1 = ArgumentCaptor.forClass(Context.class);

        given(store.execute(captor.capture(), contextCaptor1.capture())).willReturn(new ArrayList<>());

        final User user = new User.Builder()
                .userId("user")
                .opAuths("opAuth1", "opAuth2")
                .build();
        // When / Then
        graph.execute(opChain, user);

        final List<Operation> ops = captor.getValue().getOperations();

        JsonAssert.assertEquals(new View.Builder().build().toCompactJson(),
                ((GetElements) ops.get(0)).getView().toCompactJson());
    }

    @Test
    public void shouldFillSchemaViewAndManipulateViewRemovingBlacklistedEdgeUsingUpdateViewHook(@Mock final Store store)
            throws OperationException {
        // Given
        operation = new GetElements.Builder()
                .build();

        final UpdateViewHook updateViewHook = new UpdateViewHook.Builder()
                .blackListElementGroups(Collections.singleton(TestGroups.EDGE))
                .build();

        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));
        given(clonedOpChain.flatten()).willReturn(Arrays.asList(operation));

        given(store.getSchema()).willReturn(new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition())
                .edge(TestGroups.EDGE_5, new SchemaEdgeDefinition())
                .build());
        given(store.getProperties()).willReturn(new StoreProperties());

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(updateViewHook)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .build();

        final ArgumentCaptor<OperationChain> captor = ArgumentCaptor.forClass(OperationChain.class);
        final ArgumentCaptor<Context> contextCaptor1 = ArgumentCaptor.forClass(Context.class);

        given(store.execute(captor.capture(), contextCaptor1.capture())).willReturn(new ArrayList<>());

        // When / Then
        graph.execute(opChain, user);

        final List<Operation> ops = captor.getValue().getOperations();

        JsonAssert.assertEquals(new View.Builder().edge(TestGroups.EDGE_5).build().toCompactJson(),
                ((GetElements) ops.get(0)).getView().toCompactJson());
    }

    @Test
    public void shouldFillSchemaViewAndManipulateViewRemovingBlacklistedEdgeLeavingEmptyViewUsingUpdateViewHook(
            @Mock final Store store)
            throws OperationException {
        // Given
        operation = new GetElements.Builder()
                .build();

        final UpdateViewHook updateViewHook = new UpdateViewHook.Builder()
                .blackListElementGroups(Collections.singleton(TestGroups.EDGE))
                .build();

        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));
        given(clonedOpChain.flatten()).willReturn(Arrays.asList(operation));

        given(store.getSchema()).willReturn(new Schema.Builder().edge(TestGroups.EDGE_5, new SchemaEdgeDefinition())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition()).build());
        given(store.getProperties()).willReturn(new StoreProperties());

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(updateViewHook)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .build();

        final ArgumentCaptor<OperationChain> captor = ArgumentCaptor.forClass(OperationChain.class);
        final ArgumentCaptor<Context> contextCaptor1 = ArgumentCaptor.forClass(Context.class);

        given(store.execute(captor.capture(), contextCaptor1.capture())).willReturn(new ArrayList<>());

        // When / Then
        graph.execute(opChain, user);

        final List<Operation> ops = captor.getValue().getOperations();

        JsonAssert.assertEquals(new View.Builder().edge(TestGroups.EDGE_5).build().toCompactJson(),
                ((GetElements) ops.get(0)).getView().toCompactJson());
    }

    @Test
    public void shouldCorrectlyAddExtraGroupsFromSchemaViewWithUpdateViewHookWhenNotInBlacklist(@Mock final Store store)
            throws OperationException {
        // Given
        operation = new GetElements.Builder()
                .build();

        final UpdateViewHook updateViewHook = new UpdateViewHook.Builder()
                .addExtraGroups(true)
                .blackListElementGroups(Collections.singleton(TestGroups.EDGE))
                .build();

        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));
        given(clonedOpChain.flatten()).willReturn(Arrays.asList(operation));

        given(store.getSchema()).willReturn(new Schema.Builder()
                .edge(TestGroups.EDGE_4, new SchemaEdgeDefinition())
                .edge(TestGroups.EDGE_5, new SchemaEdgeDefinition())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition())
                .build());
        given(store.getProperties()).willReturn(new StoreProperties());

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(updateViewHook)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .build();

        final ArgumentCaptor<OperationChain> captor = ArgumentCaptor.forClass(OperationChain.class);
        final ArgumentCaptor<Context> contextCaptor1 = ArgumentCaptor.forClass(Context.class);

        given(store.execute(captor.capture(), contextCaptor1.capture())).willReturn(new ArrayList<>());

        // When / Then
        graph.execute(opChain, user);

        final List<Operation> ops = captor.getValue().getOperations();

        JsonAssert.assertEquals(
                new View.Builder().edge(TestGroups.EDGE_5).edge(TestGroups.EDGE_4).build().toCompactJson(),
                ((GetElements) ops.get(0)).getView().toCompactJson());
    }

    @Test
    public void shouldNotAddExtraGroupsFromSchemaViewWithUpdateViewHookWhenInBlacklist(@Mock final Store store)
            throws OperationException {
        // Given
        operation = new GetElements.Builder()
                .build();

        final UpdateViewHook updateViewHook = new UpdateViewHook.Builder()
                .addExtraGroups(true)
                .blackListElementGroups(Sets.newHashSet(TestGroups.EDGE_4,
                        TestGroups.EDGE))
                .build();

        given(opChain.shallowClone()).willReturn(clonedOpChain);
        given(clonedOpChain.getOperations()).willReturn(Lists.newArrayList(operation));
        given(clonedOpChain.flatten()).willReturn(Arrays.asList(operation));

        given(store.getSchema()).willReturn(new Schema.Builder()
                .edge(TestGroups.EDGE_4, new SchemaEdgeDefinition())
                .edge(TestGroups.EDGE_5, new SchemaEdgeDefinition())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition())
                .build());
        given(store.getProperties()).willReturn(new StoreProperties());

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .addHook(updateViewHook)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .build();

        final ArgumentCaptor<OperationChain> captor = ArgumentCaptor.forClass(OperationChain.class);
        final ArgumentCaptor<Context> contextCaptor1 = ArgumentCaptor.forClass(Context.class);

        given(store.execute(captor.capture(), contextCaptor1.capture())).willReturn(new ArrayList<>());

        // When / Then
        graph.execute(opChain, user);

        final List<Operation> ops = captor.getValue().getOperations();

        JsonAssert.assertEquals(new View.Builder().edge(TestGroups.EDGE_5).build().toCompactJson(),
                ((GetElements) ops.get(0)).getView().toCompactJson());
    }

    @Test
    public void shouldAddSchemaGroupsIfNotIncludedInJob(@Mock final Store store) throws OperationException {
        // given
        final Job job = new Job(null, new OperationChain.Builder().first(new GetAllElements()).build());

        given(store.getSchema()).willReturn(new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition())
                .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition())
                .build());
        given(store.getProperties()).willReturn(new StoreProperties());

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .build();

        final ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        final ArgumentCaptor<Context> contextCaptor = ArgumentCaptor.forClass(Context.class);

        given(store.executeJob(jobCaptor.capture(), contextCaptor.capture())).willReturn(new JobDetail());

        // when
        graph.executeJob(job, context);

        // then
        final GetAllElements operation = (GetAllElements) ((OperationChain) jobCaptor.getValue().getOperation())
                .getOperations().get(0);

        assertEquals(new View.Builder().entity(TestGroups.ENTITY, new ViewElementDefinition())
                .edge(TestGroups.EDGE, new ViewElementDefinition())
                .edge(TestGroups.EDGE_2, new ViewElementDefinition()).build(), operation.getView());
    }

    @Test
    public void shouldAddDefaultSecurityHooksIfNotAddedToHooks() {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStore.class.getName());
        given(TestStore.mockStore.isSupported(NamedOperation.class)).willReturn(true);
        // When
        final GraphConfig config = new GraphConfig.Builder()
                .graphId("test")
                .build();

        final Graph graph = new Graph.Builder()
                .addSchemas(StreamUtil.schemas(getClass()))
                .storeProperties(storeProperties)
                .config(config)
                .build();

        // Then
        assertEquals(Arrays.asList(NamedOperationResolver.class, NamedViewResolver.class, FunctionAuthoriser.class),
                graph.getGraphHooks());
        assertEquals(CreateObject.class,
                ((FunctionAuthoriser) graph.getConfig().getHooks().get(2)).getUnauthorisedFunctions().get(0));
    }

    @Test
    public void shouldNotAddDefaultSecurityHooksIfAlreadyAdded() {
        // Given
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStore.class.getName());
        given(TestStore.mockStore.isSupported(NamedOperation.class)).willReturn(true);
        // When
        final GraphConfig config = new GraphConfig.Builder()
                .graphId("test")
                .addHook(new FunctionAuthoriser(Lists.newArrayList(Identity.class)))
                .build();

        final Graph graph = new Graph.Builder()
                .addSchemas(StreamUtil.schemas(getClass()))
                .storeProperties(storeProperties)
                .config(config)
                .build();

        // Then
        assertEquals(Arrays.asList(NamedOperationResolver.class, NamedViewResolver.class, FunctionAuthoriser.class),
                graph.getGraphHooks());
        assertEquals(Identity.class,
                ((FunctionAuthoriser) graph.getConfig().getHooks().get(2)).getUnauthorisedFunctions().get(0));
    }

    @Test
    public void shouldExpandGlobalEdges(@Mock final Store store, @Mock final ElementFilter filter,
            @Mock final StoreProperties mockStoreProperties)
            throws OperationException {

        final Schema twoEdgesNoEntities = new Schema.Builder()
                .type(TestTypes.PROP_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("vertex", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .edge("firstEdge",
                        new SchemaEdgeDefinition.Builder()
                                .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                                .aggregate(false)
                                .source("vertex")
                                .destination("vertex").directed(DIRECTED_EITHER)
                                .build())
                .edge("secondEdge",
                        new SchemaEdgeDefinition.Builder()
                                .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                                .aggregate(false)
                                .source("vertex")
                                .destination("vertex")
                                .directed(DIRECTED_EITHER)
                                .build())
                .build();

        final ArgumentCaptor<OperationChain> capturedOperation = ArgumentCaptor.forClass(OperationChain.class);
        final ArgumentCaptor<Context> capturedContext = ArgumentCaptor.forClass(Context.class);

        given(store.getSchema()).willReturn(twoEdgesNoEntities);
        given(store.getProperties()).willReturn(mockStoreProperties);

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(twoEdgesNoEntities)
                .build();

        final GlobalViewElementDefinition globalEdgeAggregate = new GlobalViewElementDefinition.Builder()
                .postAggregationFilter(filter)
                .build();
        final View view = new View.Builder()
                .globalEdges(globalEdgeAggregate)
                .build();

        operation = new GetElements.Builder()
                .view(view)
                .build();
        opChain = new OperationChain.Builder()
                .first(operation)
                .build();

        graph.execute(opChain, context);
        Mockito.verify(store, Mockito.times(1)).execute(capturedOperation.capture(), capturedContext.capture());

        assertEquals(1, capturedOperation.getAllValues().size());
        final OperationChain transformedOpChain = capturedOperation.getAllValues().get(0);

        assertEquals(1, transformedOpChain.getOperations().size());
        assertEquals(GetElements.class, transformedOpChain.getOperations().get(0).getClass());
        final View mergedView = ((GetElements) transformedOpChain.getOperations().get(0)).getView();
        assertTrue(mergedView.getGlobalEdges() == null || mergedView.getGlobalEdges().size() == 0);
        assertEquals(2, mergedView.getEdges().size());
        for (final Map.Entry<String, ViewElementDefinition> e : mergedView.getEdges().entrySet()) {
            assertNotNull(e.getValue().getPostAggregationFilter());
        }

    }

    @Test
    public void shouldExpandAllEdges(@Mock final Store store, @Mock final ElementFilter filter,
            @Mock final StoreProperties mockStoreProperties)
            throws OperationException {
        final Schema twoEdgesNoEntities = new Schema.Builder()
                .type(TestTypes.PROP_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("vertex", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .edge("firstEdge",
                        new SchemaEdgeDefinition.Builder()
                                .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                                .aggregate(false)
                                .source("vertex")
                                .destination("vertex").directed(DIRECTED_EITHER)
                                .build())
                .edge("secondEdge",
                        new SchemaEdgeDefinition.Builder()
                                .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                                .aggregate(false)
                                .source("vertex")
                                .destination("vertex")
                                .directed(DIRECTED_EITHER)
                                .build())
                .build();

        final ArgumentCaptor<OperationChain> capturedOperation = ArgumentCaptor.forClass(OperationChain.class);
        final ArgumentCaptor<Context> capturedContext = ArgumentCaptor.forClass(Context.class);

        given(store.getSchema()).willReturn(twoEdgesNoEntities);
        given(store.getOriginalSchema()).willReturn(twoEdgesNoEntities);
        given(store.getProperties()).willReturn(mockStoreProperties);

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(twoEdgesNoEntities)
                .build();

        final GlobalViewElementDefinition globalEdgeAggregate = new GlobalViewElementDefinition.Builder()
                .postAggregationFilter(filter)
                .build();
        final View view = new View.Builder()
                .allEdges(true)
                .build();

        operation = new GetElements.Builder()
                .view(view)
                .build();
        opChain = new OperationChain.Builder()
                .first(operation)
                .build();

        graph.execute(opChain, context);
        Mockito.verify(store, Mockito.times(1)).execute(capturedOperation.capture(), capturedContext.capture());

        assertEquals(1, capturedOperation.getAllValues().size());
        final OperationChain transformedOpChain = capturedOperation.getAllValues().get(0);

        assertEquals(1, transformedOpChain.getOperations().size());
        assertEquals(GetElements.class, transformedOpChain.getOperations().get(0).getClass());
        final View mergedView = ((GetElements) transformedOpChain.getOperations().get(0)).getView();
        assertTrue(mergedView.isAllEdges());
        assertEquals(2, mergedView.getEdges().size());
    }

    @Test
    public void preserveAllEntitiesIfNoEntitiesInSchema(@Mock final Store store, @Mock final ElementFilter filter,
            @Mock final StoreProperties mockStoreProperties)
            throws OperationException {
        final Schema twoEdgesNoEntities = new Schema.Builder()
                .type(TestTypes.PROP_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("vertex", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .edge("firstEdge",
                        new SchemaEdgeDefinition.Builder()
                                .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                                .aggregate(false)
                                .source("vertex")
                                .destination("vertex").directed(DIRECTED_EITHER)
                                .build())
                .edge("secondEdge",
                        new SchemaEdgeDefinition.Builder()
                                .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                                .aggregate(false)
                                .source("vertex")
                                .destination("vertex")
                                .directed(DIRECTED_EITHER)
                                .build())
                .build();

        final ArgumentCaptor<OperationChain> capturedOperation = ArgumentCaptor.forClass(OperationChain.class);
        final ArgumentCaptor<Context> capturedContext = ArgumentCaptor.forClass(Context.class);

        given(store.getSchema()).willReturn(twoEdgesNoEntities);
        given(store.getOriginalSchema()).willReturn(twoEdgesNoEntities);
        given(store.getProperties()).willReturn(mockStoreProperties);

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(twoEdgesNoEntities)
                .build();

        final GlobalViewElementDefinition globalEdgeAggregate = new GlobalViewElementDefinition.Builder()
                .postAggregationFilter(filter)
                .build();
        final View view = new View.Builder()
                .allEntities(true)
                .build();

        operation = new GetElements.Builder()
                .view(view)
                .build();
        opChain = new OperationChain.Builder()
                .first(operation)
                .build();

        graph.execute(opChain, context);
        Mockito.verify(store, Mockito.times(1)).execute(capturedOperation.capture(), capturedContext.capture());

        assertEquals(1, capturedOperation.getAllValues().size());
        final OperationChain transformedOpChain = capturedOperation.getAllValues().get(0);

        assertEquals(1, transformedOpChain.getOperations().size());
        assertEquals(GetElements.class, transformedOpChain.getOperations().get(0).getClass());
        final View mergedView = ((GetElements) transformedOpChain.getOperations().get(0)).getView();
        assertTrue(mergedView.isAllEntities());
        assertEquals(0, mergedView.getEntities().size());
    }

    @Test
    public void shouldNotExpandGlobalEdgesWhereNotPresentInSchema(@Mock final Store store,
            @Mock final ElementFilter filter,
            @Mock final StoreProperties mockStoreProperties)
            throws OperationException {
        final Schema federatedStoreSchema = new Schema.Builder().build();

        given(store.getSchema()).willReturn(federatedStoreSchema);
        given(store.getProperties()).willReturn(mockStoreProperties);
        final ArgumentCaptor<OperationChain> capturedOperation = ArgumentCaptor.forClass(OperationChain.class);
        final ArgumentCaptor<Context> capturedContext = ArgumentCaptor.forClass(Context.class);

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .store(store)
                .addSchema(federatedStoreSchema)
                .build();

        final GlobalViewElementDefinition globalEdgeAggregate = new GlobalViewElementDefinition.Builder()
                .postAggregationFilter(filter)
                .build();
        final View view = new View.Builder()
                .globalEdges(globalEdgeAggregate)
                .build();

        operation = new GetElements.Builder()
                .view(view)
                .build();
        opChain = new OperationChain.Builder()
                .first(operation)
                .build();

        graph.execute(opChain, context);
        Mockito.verify(store, Mockito.times(1)).execute(capturedOperation.capture(), capturedContext.capture());

        assertEquals(1, capturedOperation.getAllValues().size());
        final OperationChain transformedOpChain = capturedOperation.getAllValues().get(0);

        assertEquals(1, transformedOpChain.getOperations().size());
        assertEquals(GetElements.class, transformedOpChain.getOperations().get(0).getClass());
        final View mergedView = ((GetElements) transformedOpChain.getOperations().get(0)).getView();
        assertEquals(0, mergedView.getEdges().size());
        assertEquals(1, mergedView.getGlobalEdges().size());
        assertNotNull(mergedView.getGlobalEdges().get(0).getPostAggregationFilter());

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
        protected OutputOperationHandler<GetTraits, Set<StoreTrait>> getGetTraitsHandler() {
            return new GetTraitsHandler(new HashSet<>(0));
        }

        @Override
        protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
            return ToBytesSerialiser.class;
        }
    }

    private static class ViewCheckerGraphHook implements GraphHook {
        @Override
        public void preExecute(final OperationChain<?> opChain, final Context context) {
            for (final Operation operation : opChain.getOperations()) {
                if (operation instanceof OperationView && null == ((OperationView) operation).getView()) {
                    throw new IllegalArgumentException("View should not be null");
                }
            }
        }

        @Override
        public <T> T postExecute(final T result, final OperationChain<?> opChain, final Context context) {
            return result;
        }

        @Override
        public <T> T onFailure(final T result, final OperationChain<?> opChain, final Context context,
                final Exception e) {
            return result;
        }
    }
}
