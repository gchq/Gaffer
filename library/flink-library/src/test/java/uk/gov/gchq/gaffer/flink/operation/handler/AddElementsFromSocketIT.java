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

package uk.gov.gchq.gaffer.flink.operation.handler;

import org.junit.Test;
import uk.gov.gchq.gaffer.flink.operation.AddElementsFromSocket;
import uk.gov.gchq.gaffer.flink.operation.FlinkTest;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class AddElementsFromSocketIT extends FlinkTest {
    @Test
    public void shouldAddElementsFromFile() throws Exception {
        // Given
        final Graph graph = createGraph();
        final boolean validate = true;
        final boolean skipInvalid = false;
        final String hostname = "localhost";
        final int port = 6666;

        final AddElementsFromSocket op = new AddElementsFromSocket.Builder()
                .jobName("test import from topic")
                .generator(BasicGenerator.class)
                .parallelism(1)
                .validate(validate)
                .skipInvalidElements(skipInvalid)
                .hostname(hostname)
                .port(port)
                .build();

        new Thread(() -> {
            try (final ServerSocket server = new ServerSocket(6666);
                 final Socket socket = server.accept();
                 final OutputStream out = socket.getOutputStream()) {
                out.write(DATA_BYTES);
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }).start();

        // When
        graph.execute(op, new User());

        // Then
        verifyElements(graph);
    }
}
