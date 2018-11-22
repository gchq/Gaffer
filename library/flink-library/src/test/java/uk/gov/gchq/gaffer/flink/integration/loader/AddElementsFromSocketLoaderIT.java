/*
 * Copyright 2018 Crown Copyright
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

import org.junit.After;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.integration.impl.loader.ParameterizedLoaderIT;
import uk.gov.gchq.gaffer.integration.impl.loader.schemas.SchemaLoader;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromSocket;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.TestSchema;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

public class AddElementsFromSocketLoaderIT extends ParameterizedLoaderIT<AddElementsFromSocket> {

    final String hostname = "localhost";
    final int[] port = new int[1];
    ServerSocket server = null;

    @After
    public void after() throws IOException {
        if (null != server) {
            server.close();
            server = null;
        }
    }

    public AddElementsFromSocketLoaderIT(final TestSchema schema, final SchemaLoader loader, final Map<String, User> userMap) {
        super(schema, loader, userMap);
        StoreProperties props = getStoreProperties();
        props.addOperationDeclarationPaths("../../library/flink-library/src/main/resources/FlinkOperationDeclarations.json");
        setStoreProperties(props);
    }

    @Override
    protected void addElements(final Iterable<? extends Element> input) throws OperationException {
        try {
            configure(input);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        graph.execute(new AddElementsFromSocket.Builder()
                .generator(GeneratorImpl.class)
                .parallelism(1)
                .validate(false)
                .skipInvalidElements(false)
                .hostname(hostname)
                .port(port[0])
                .build(), getUser());
    }

    private void configure(final Iterable<? extends Element> elements) throws Exception {
        server = new ServerSocket(0);
        port[0] = server.getLocalPort();

        new Thread(() -> {
            try (final Socket socket = server.accept();
                 final OutputStream out = socket.getOutputStream()) {

                final StringBuilder builder = new StringBuilder();

                for (final Element element : elements) {
                    if (element instanceof Entity) {
                        builder.append(((Entity) element).getVertex() + "\n");
                    }
                }
                out.write(StringUtil.toBytes(builder.toString()));
            } catch (final IOException ex) {
                throw new RuntimeException();
            }
        }).start();
    }
}