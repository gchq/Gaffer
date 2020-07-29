/*
 * Copyright 2020 Crown Copyright
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
package uk.gov.gchq.gaffer.operation.runner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.MapReduce;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.TextJobInitialiser;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.JsonMapperGenerator;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.user.User;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class OperationRunnerTest {

    private static final String GRAPH_ID = "graphId";
    private static final User UNKNOWN_USER = new User();
    private static final User USER = new User.Builder()
            .userId("userId")
            .dataAuth("dataAuth")
            .opAuth("opAuth")
            .build();
    private static final GetAllElements GET_ALL_ELEMENTS_OPERATION = new GetAllElements.Builder()
            .directedType(DirectedType.EITHER)
            .build();
    private static final AddElementsFromHdfs ADD_ELEMENTS_FROM_HDFS_OPERATION = new AddElementsFromHdfs.Builder()
            .workingPath("/tmp/working")
            .outputPath("/output")
            .failurePath("/tmp/failure")
            .splitsFilePath("/tmp/splits/splitsFile")
            .addInputMapperPair("/data", JsonMapperGenerator.class.getName())
            .jobInitialiser(new TextJobInitialiser())
            .build();

    @TempDir
    Path tempDir;

    private Graph.Builder graphBuilder;
    private String operationChainPath;

    private String storePropertiesPath;
    private String schemaPath;
    private String userPath;
    private OperationRunner operationRunner;
    private Operation operation;

    @BeforeEach
    public void setUp() throws IOException {
        graphBuilder = mock(Graph.Builder.class);

        storePropertiesPath = Files.createTempFile(tempDir, null, null).toString();
        schemaPath = Files.createTempFile(tempDir, null, null).toString();
        userPath = createFileContaining(tempDir, JSONSerialiser.serialise(USER)).getPath();

        lenient().when(graphBuilder.storeProperties(storePropertiesPath)).thenReturn(graphBuilder);
        lenient().when(graphBuilder.addSchemas(any(Path.class))).thenReturn(graphBuilder);
        lenient().when(graphBuilder.config(any(GraphConfig.class))).thenReturn(graphBuilder);

        operationRunner = new TestOperationRunner(graphBuilder);
    }

    private void setUpOperation(final Path tempDir, final Operation operation) throws IOException {
        this.operation = operation;
        this.operationChainPath = createFileContaining(tempDir, JSONSerialiser.serialise(operation)).getPath();
    }

    private File createFileContaining(final Path tempDir, final byte[] content) throws IOException {
        return createFileContaining(tempDir, new ByteArrayInputStream(content));
    }

    private File createFileContaining(final Path tempDir, final InputStream contentStream) throws IOException {
        final File contentFile = Files.createTempFile(tempDir, null, null).toFile();
        Files.copy(contentStream, contentFile.toPath(), REPLACE_EXISTING);
        return contentFile;
    }

    @Test
    public void shouldRunOperationUsingSuppliedParametersAndUser() throws IOException {
        setUpOperation(tempDir, GET_ALL_ELEMENTS_OPERATION);

        final String[] args = new String[]{
                "--operation-chain", operationChainPath,
                "--store-properties", storePropertiesPath,
                "--schema", schemaPath,
                "--user", userPath,
                "--graph-id", GRAPH_ID
        };

        OperationRunner.run(operationRunner, args);

        checkGraphBuilderCreation();
        checkExpectedUser(USER);
        checkExpectedOperation();
    }

    @Test
    public void shouldRunOperationUsingSuppliedParametersAndDefaultUser() throws IOException {
        setUpOperation(tempDir, GET_ALL_ELEMENTS_OPERATION);

        final String[] args = new String[]{
                "--operation-chain", operationChainPath,
                "--store-properties", storePropertiesPath,
                "--schema", schemaPath,
                "--graph-id", GRAPH_ID
        };

        OperationRunner.run(operationRunner, args);

        checkGraphBuilderCreation();
        checkExpectedUser(UNKNOWN_USER);
        checkExpectedOperation();
    }

    @Test
    public void shouldConfigureCommandLineArgsForMapReduceOperations() throws IOException {
        setUpOperation(tempDir, ADD_ELEMENTS_FROM_HDFS_OPERATION);

        final String[] expectedArgs = new String[]{
                "--operation-chain", operationChainPath,
                "--store-properties", storePropertiesPath,
                "--schema", schemaPath,
                "--graph-id", GRAPH_ID
        };

        final String[] args = new String[expectedArgs.length + 1];
        args[0] = OperationRunner.class.getName();
        System.arraycopy(expectedArgs, 0, args, 1, expectedArgs.length);

        OperationRunner.run(operationRunner, args);

        checkGraphBuilderCreation();
        checkExpectedUser(UNKNOWN_USER);
        MapReduce.class.cast(operation).setCommandLineArgs(expectedArgs);
        checkExpectedOperation();
        checkExpectedArgs(expectedArgs);
    }

    private class TestOperationRunner extends OperationRunner {
        private final Graph.Builder graphBuilder;

        TestOperationRunner(final Graph.Builder graphBuilder) {
            this.graphBuilder = graphBuilder;
        }

        @Override
        protected Graph.Builder createGraphBuilder() {
            return graphBuilder;
        }

        @Override
        protected Object execute(final Graph.Builder graphBuilder) {
            /* Intentional workaround for inability to mock final classes in Mockito 1.x */
            return null;
        }
    }

    private void checkGraphBuilderCreation() {
        final ArgumentCaptor<GraphConfig> graphConfigArgumentCaptor = ArgumentCaptor.forClass(GraphConfig.class);
        final ArgumentCaptor<Path> schemaPathArgumentCaptor = ArgumentCaptor.forClass(Path.class);

        verify(graphBuilder).storeProperties(storePropertiesPath);
        verify(graphBuilder).addSchemas(schemaPathArgumentCaptor.capture());
        verify(graphBuilder).config(graphConfigArgumentCaptor.capture());

        assertEquals(GRAPH_ID, graphConfigArgumentCaptor.getValue().getGraphId());
        assertEquals(schemaPath, schemaPathArgumentCaptor.getValue().toString());
    }

    private void checkExpectedOperation() throws SerialisationException {
        assertArrayEquals(
                JSONSerialiser.serialise(OperationChain.wrap(operation)),
                JSONSerialiser.serialise(operationRunner.getOperationChain()));
    }

    private void checkExpectedUser(final User user) {
        assertEquals(user, operationRunner.getUser());
    }

    private void checkExpectedArgs(final String[] expectedArgs) {
        assertArrayEquals(expectedArgs, ((MapReduce) operationRunner.getOperationChain().getOperations().get(0)).getCommandLineArgs());
    }
}
