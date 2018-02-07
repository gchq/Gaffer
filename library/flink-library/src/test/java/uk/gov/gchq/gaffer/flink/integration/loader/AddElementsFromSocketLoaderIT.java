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

package uk.gov.gchq.gaffer.flink.integration.loader;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.integration.generators.JsonToElementGenerator;
import uk.gov.gchq.gaffer.integration.impl.loader.AbstractLoaderIT;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromSocket;

import java.net.ServerSocket;

public class AddElementsFromSocketLoaderIT extends AbstractLoaderIT<AddElementsFromSocket> {

    final String hostname = "localhost";
    final int[] port = new int[1];

    @Override
    protected void configure(final Iterable<? extends Element> elements) throws Exception {
        final ServerSocket server = new ServerSocket(0);
        port[0] = server.getLocalPort();

//        new Thread(() -> {
//            try (final Socket socket = server.accept();
//                 final OutputStream out = socket.getOutputStream()) {
//                out.write(DATA_BYTES);
//            } catch (IOException e) {
//                throw new RuntimeException();
//            }
//        }).start();
    }

    @Override
    protected AddElementsFromSocket createOperation(final Iterable<? extends Element> elements) {
        return new AddElementsFromSocket.Builder()
                .generator(JsonToElementGenerator.class)
                .parallelism(1)
                .validate(true)
                .skipInvalidElements(false)
                .hostname(hostname)
                .port(port[0])
                .build();
    }
}
