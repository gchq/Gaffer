/*
 * Copyright 2017-2021 Crown Copyright
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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.glassfish.jersey.server.ChunkedOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.exception.UnauthorisedException;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.core.exception.Error;
import uk.gov.gchq.gaffer.core.exception.Status;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.rest.factory.ExamplesFactory;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.rest.model.OperationDetail;
import uk.gov.gchq.gaffer.store.Context;

import javax.inject.Inject;
import javax.ws.rs.core.Response;

import java.io.IOException;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser.createDefaultMapper;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.JOB_ID_HEADER;

/**
 * An implementation of {@link IOperationServiceV2}. By default it will use a singleton
 * {@link uk.gov.gchq.gaffer.graph.Graph} generated using the {@link uk.gov.gchq.gaffer.rest.factory.GraphFactory}.
 * All operations are simple delegated to the graph.
 * Pre and post operation hooks are available by extending this class and implementing preOperationHook and/or
 * postOperationHook.
 */
public class OperationServiceV2 extends AbstractOperationService implements IOperationServiceV2 {
    private static final Logger LOGGER = LoggerFactory.getLogger(OperationServiceV2.class);

    @Inject
    private GraphFactory graphFactory;

    @Inject
    private UserFactory userFactory;

    @Inject
    private ExamplesFactory examplesFactory;

    public final ObjectMapper mapper = createDefaultMapper();

    @Override
    public Response getOperations() {
        return Response.ok(graphFactory.getGraph().getSupportedOperations())
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .build();
    }

    @Override
    public Response getOperationDetails() {
        return Response.ok(getSupportedOperationDetails())
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .build();
    }

    @Override
    public Response execute(final Operation operation) {
        final Pair<Object, String> resultAndJobId = _execute(operation, userFactory.createContext());
        return Response.ok(resultAndJobId.getFirst())
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .header(JOB_ID_HEADER, resultAndJobId.getSecond())
                .build();
    }

    @Override
    public Response executeChunked(final Operation operation) {
        return executeChunkedChain(OperationChain.wrap(operation));
    }

    @SuppressFBWarnings
    @Override
    public Response executeChunkedChain(final OperationChain opChain) {
        // Create chunked output instance
        final Throwable[] threadException = new Throwable[1];
        final ChunkedOutput<String> output = new ChunkedOutput<>(String.class, "\r\n");
        final Context context = userFactory.createContext();

        // create thread to write chunks to the chunked output object
        Thread thread = new Thread(() -> {
            try {
                final Object result = _execute(opChain, context).getFirst();
                chunkResult(result, output);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            } finally {
                CloseableUtil.close(output);
                CloseableUtil.close(opChain);
            }
        });

        // By default threads throw nothing, so set the ExceptionHandler
        thread.setUncaughtExceptionHandler((thread1, exception) -> threadException[0] = exception.getCause());

        thread.start();

        // Sleep to check exception will be caught
        try {
            Thread.sleep(1000);
        } catch (final InterruptedException e) {
            return Response.status(INTERNAL_SERVER_ERROR)
                    .entity(new Error.ErrorBuilder()
                            .status(Status.INTERNAL_SERVER_ERROR)
                            .statusCode(500)
                            .simpleMessage(e.getMessage())
                            .build())
                    .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                    .build();
        }

        // If there was an UnauthorisedException thrown return 403, else return a 500
        if (null != threadException[0]) {
            if (threadException.getClass().equals(UnauthorisedException.class)) {
                return Response.status(INTERNAL_SERVER_ERROR)
                        .entity(new Error.ErrorBuilder()
                                .status(Status.FORBIDDEN)
                                .statusCode(403)
                                .simpleMessage(threadException[0].getMessage())
                                .build())
                        .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                        .build();
            } else {
                return Response.status(INTERNAL_SERVER_ERROR)
                        .entity(new Error.ErrorBuilder()
                                .status(Status.INTERNAL_SERVER_ERROR)
                                .statusCode(500)
                                .simpleMessage(threadException[0].getMessage())
                                .build())
                        .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                        .build();
            }
        }

        // Return ok output
        return Response.ok(output)
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .build();
    }

    @Override
    public Response operationDetails(final String className) throws InstantiationException, IllegalAccessException {
        try {
            final Class<? extends Operation> operationClass = getOperationClass(className);

            if (graphFactory.getGraph().getSupportedOperations().contains(operationClass)) {
                return Response.ok(new OperationDetail(operationClass, getNextOperations(operationClass), generateExampleJson(operationClass)))
                        .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                        .build();
            } else {
                LOGGER.info("Class: {} was found on the classpath, but is not supported by the current store.", className);
                return Response.status(NOT_FOUND)
                        .entity(new Error.ErrorBuilder()
                                .status(Status.NOT_FOUND)
                                .statusCode(404)
                                .simpleMessage("Class: " + className + " is not supported by the current store.")
                                .detailMessage("Class: " + className + " was found on the classpath," +
                                        "but is not supported by the current store.")
                                .build())
                        .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                        .build();
            }
        } catch (final ClassNotFoundException e) {
            LOGGER.info("Class: {} was not found on the classpath.", className, e);
            return Response.status(NOT_FOUND)
                    .entity(new Error.ErrorBuilder()
                            .status(Status.NOT_FOUND)
                            .statusCode(404)
                            .simpleMessage("Class: " + className + " was not found on the classpath.")
                            .build())
                    .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                    .build();
        }
    }

    @Override
    public Response operationExample(final String className) throws InstantiationException, IllegalAccessException {
        try {
            return Response.ok(generateExampleJson(getOperationClass(className)))
                    .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                    .build();
        } catch (final ClassNotFoundException e) {
            LOGGER.info("Class: {} was not found on the classpath.", className, e);
            return Response.status(NOT_FOUND)
                    .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                    .build();
        } catch (final IllegalAccessException | InstantiationException e) {
            LOGGER.info("Unable to create example JSON for class: {}.", className, e);
            throw e;
        }
    }

    @Override
    public Response nextOperations(final String operationClassName) {
        Class<? extends Operation> opClass;
        try {
            opClass = getOperationClass(operationClassName);
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException("Operation class was not found: " + operationClassName, e);
        } catch (final ClassCastException e) {
            throw new IllegalArgumentException(operationClassName + " does not extend Operation", e);
        }

        return Response.ok(getNextOperations(opClass))
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .build();
    }

    @Override
    protected UserFactory getUserFactory() {
        return userFactory;
    }

    @Override
    protected ExamplesFactory getExamplesFactory() {
        return examplesFactory;
    }

    @Override
    protected GraphFactory getGraphFactory() {
        return graphFactory;
    }

    protected void preOperationHook(final OperationChain<?> opChain, final Context context) {
        // no action by default
    }

    protected void postOperationHook(final OperationChain<?> opChain, final Context context) {
        // no action by default
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
