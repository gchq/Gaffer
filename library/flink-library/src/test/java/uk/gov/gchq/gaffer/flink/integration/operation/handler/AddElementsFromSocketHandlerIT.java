/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.flink.integration.operation.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.flink.operation.FlinkTest;
import uk.gov.gchq.gaffer.flink.operation.TestFileSink;
import uk.gov.gchq.gaffer.flink.operation.handler.AddElementsFromSocketHandler;
import uk.gov.gchq.gaffer.generator.TestBytesGeneratorImpl;
import uk.gov.gchq.gaffer.generator.TestGeneratorImpl;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromSocket;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class AddElementsFromSocketHandlerIT extends FlinkTest {

    private TestFileSink testFileSink;

    @BeforeEach
    public void create() throws IOException {
        testFileSink = createTestFileSink();
    }

    @Test
    public void shouldAddElements() throws Exception {
        // Given
        MapStore.resetStaticMap();
        final Graph graph = createGraph();
        final boolean validate = true;
        final boolean skipInvalid = false;
        final String hostname = "localhost";
        final int[] port = new int[1];

        final ServerSocket server = new ServerSocket(0);
        port[0] = server.getLocalPort();

        new Thread(() -> {
            try (final Socket socket = server.accept();
                 final OutputStream out = socket.getOutputStream()) {
                out.write(DATA_BYTES);
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }).start();

        final AddElementsFromSocket op = new AddElementsFromSocket.Builder()
                .generator(TestGeneratorImpl.class)
                .parallelism(1)
                .validate(validate)
                .skipInvalidElements(skipInvalid)
                .hostname(hostname)
                .port(port[0])
                .build();

        // When
        graph.execute(op, new User());

        // Then
        verifyElements(byte[].class, testFileSink, TestBytesGeneratorImpl.class);
    }

    @Override
    public Store createStore() {
        final Store store = Store.createStore("graphId", SCHEMA, MapStoreProperties.loadStoreProperties("store.properties"));
        store.addOperationHandler(AddElementsFromSocket.class, new AddElementsFromSocketHandler(testFileSink));
        return store;
    }

}
