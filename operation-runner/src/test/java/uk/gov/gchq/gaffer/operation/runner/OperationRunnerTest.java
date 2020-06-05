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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OperationRunnerTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Graph.Builder graphBuilder;
    private String operationChainPath;
    private String storePropertiesPath;
    private String schemaPath;
    private String userPath;
    private OperationRunner operationRunner;

    private final String graphId = "graphId";
    private final User unknownUser = new User();
    private final User user = new User.Builder()
            .userId("userId")
            .dataAuth("dataAuth")
            .opAuth("opAuth")
            .build();
    private final GetAllElements operation = new GetAllElements.Builder()
            .directedType(DirectedType.EITHER)
            .build();

    @Before
    public void setUp() throws IOException {
        graphBuilder = mock(Graph.Builder.class);

        operationChainPath = createFileContaining(JSONSerialiser.serialise(operation)).getPath();
        storePropertiesPath = temporaryFolder.newFile().getPath();
        schemaPath = temporaryFolder.newFile().getPath();
        userPath = createFileContaining(JSONSerialiser.serialise(user)).getPath();

        when(graphBuilder.storeProperties(storePropertiesPath)).thenReturn(graphBuilder);
        when(graphBuilder.addSchemas(any(Path.class))).thenReturn(graphBuilder);
        when(graphBuilder.config(any(GraphConfig.class))).thenReturn(graphBuilder);

        operationRunner = new TestOperationRunner(graphBuilder);
    }

    private File createFileContaining(final byte[] content) throws IOException {
        return createFileContaining(new ByteArrayInputStream(content));
    }

    private File createFileContaining(final InputStream contentStream) throws IOException {
        final File contentFile = temporaryFolder.newFile();
        Files.copy(contentStream, contentFile.toPath(), REPLACE_EXISTING);
        return contentFile;
    }

    @Test
    public void shouldRunOperationUsingSuppliedParametersAndUser() throws SerialisationException {
        final String[] args = new String[]{
                "--operation-chain", operationChainPath,
                "--store-properties", storePropertiesPath,
                "--schema", schemaPath,
                "--user", userPath,
                "--graph-id", graphId
        };

        OperationRunner.run(operationRunner, args);

        checkGraphBuilderCreation();
        checkExpectedUser(user);
        checkExpectedOperation();
        checkExpectedArgs(args);
    }

    @Test
    public void shouldRunOperationUsingSuppliedParametersAndDefaultUser() throws SerialisationException {
        final String[] args = new String[]{
                "--operation-chain", operationChainPath,
                "--store-properties", storePropertiesPath,
                "--schema", schemaPath,
                "--graph-id", graphId
        };

        OperationRunner.run(operationRunner, args);

        checkGraphBuilderCreation();
        checkExpectedUser(unknownUser);
        checkExpectedOperation();
        checkExpectedArgs(args);
    }

    @Test
    public void shouldStripRedundantArgs() throws SerialisationException {
        final String[] expectedArgs = new String[]{
                "--operation-chain", operationChainPath,
                "--store-properties", storePropertiesPath,
                "--schema", schemaPath,
                "--graph-id", graphId
        };

        final String[] args = new String[expectedArgs.length + 1];
        args[0] = OperationRunner.class.getName();
        System.arraycopy(expectedArgs, 0, args, 1, expectedArgs.length);

        OperationRunner.run(operationRunner, args);

        checkGraphBuilderCreation();
        checkExpectedUser(unknownUser);
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

        assertEquals(graphId, graphConfigArgumentCaptor.getValue().getGraphId());
        assertEquals(schemaPath, schemaPathArgumentCaptor.getValue().toString());
    }

    private void checkExpectedOperation() throws SerialisationException {
        assertArrayEquals(
                JSONSerialiser.serialise(OperationChain.wrap(operation)),
                JSONSerialiser.serialise(operationRunner.getOperationChain()));
    }

    private void checkExpectedUser(final User user) {
        assertEquals(user, operationRunner.getContext().getUser());
    }

    private void checkExpectedArgs(final String[] expectedArgs) {
        assertArrayEquals(expectedArgs, operationRunner.getContext().getCommandLineArgs());
    }
}
