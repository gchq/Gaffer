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
package uk.gov.gchq.gaffer.rest.service.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.jersey.server.ChunkedOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.user.User;
import javax.inject.Inject;
import javax.ws.rs.core.Response;
import java.io.IOException;

import static uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser.createDefaultMapper;
import static uk.gov.gchq.gaffer.rest.service.v2.ServiceConstants.GAFFER_MEDIA_TYPE;
import static uk.gov.gchq.gaffer.rest.service.v2.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;

public class OperationServiceV2 implements IOperationServiceV2 {
    private static final Logger LOGGER = LoggerFactory.getLogger(OperationServiceV2.class);
    public final ObjectMapper mapper = createDefaultMapper();

    @Inject
    private GraphFactory graphFactory;

    @Inject
    private UserFactory userFactory;

    @Override
    public Response getOperations() {
        return Response.ok(graphFactory.getGraph().getSupportedOperations())
                       .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                       .build();
    }

    @Override
    public Response execute(final Operation operation) {
        return _executeRest(operation);
    }

    @Override
    public Response execute(final OperationChain opChain) {
        return _executeRest(opChain);
    }

    @Override
    public ChunkedOutput<String> executeChunked(final Operation operation) {
        return executeChunked(new OperationChain(operation));
    }

    @Override
    public ChunkedOutput<String> executeChunked(final OperationChain<CloseableIterable<Element>> opChain) {
        // Create chunked output instance
        final ChunkedOutput<String> output = new ChunkedOutput<>(String.class, "\r\n");

        // write chunks to the chunked output object
        new Thread() {
            @Override
            public void run() {
                try {
                    final Object result = _execute(opChain);
                    chunkResult(result, output);
                } finally {
                    CloseableUtil.close(output);
                    CloseableUtil.close(opChain);
                }
            }
        }.start();

        return output;
    }

    @Override
    public Response generateObjects(final GenerateObjects<Object> operation) {
        return _executeRest(operation);
    }

    @Override
    public Response generateElements(final GenerateElements<Object> operation) {
        return _executeRest(operation);
    }

    @Override
    public Response getAdjacentIds(final GetAdjacentIds operation) {
        return _executeRest(operation);
    }

    @Override
    public Response getAllElements(final GetAllElements operation) {
        return _executeRest(operation);
    }

    @Override
    public Response getElements(final GetElements operation) {
        return _executeRest(operation);
    }

    @Override
    public Response addElements(final AddElements operation) {
        _execute(operation);

        return Response.noContent()
                       .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                       .build();
    }

    protected void preOperationHook(final OperationChain<?> opChain, final User user) {
        // no action by default
    }

    protected void postOperationHook(final OperationChain<?> opChain, final User user) {
        // no action by default
    }

    private Response _executeRest(final Operation operation) {
        return _executeRest(new OperationChain(operation));
    }

    private Response _executeRest(final OperationChain opChain) {
        return Response.ok(_execute(opChain))
                       .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                       .build();
    }

    protected <O> O _execute(final Operation operation) {
        return _execute(new OperationChain<>(operation));
    }

    @SuppressWarnings("ThrowFromFinallyBlock")
    protected <O> O _execute(final OperationChain<O> opChain) {
        final User user = userFactory.createUser();
        preOperationHook(opChain, user);

        O result;
        try {
            result = graphFactory.getGraph().execute(opChain, user);
        } catch (final OperationException e) {
            CloseableUtil.close(opChain);
            throw new RuntimeException("Error executing opChain", e);
        } finally {
            try {
                postOperationHook(opChain, user);
            } catch (final Exception e) {
                CloseableUtil.close(opChain);
                throw e;
            }
        }

        return result;
    }

    protected void chunkResult(final Object result, final ChunkedOutput<String> output) {
        if (result instanceof Iterable) {
            final Iterable itr = (Iterable) result;
            try {
                for (final Object item : itr) {
                    output.write(mapper.writeValueAsString(item));
                }
            } catch (final IOException ioe) {
                LOGGER.warn("IOException (chunks)", ioe);
            } finally {
                CloseableUtil.close(itr);
            }
        } else {
            try {
                output.write(mapper.writeValueAsString(result));
            } catch (final IOException ioe) {
                LOGGER.warn("IOException (chunks)", ioe);
            }
        }
    }
}
