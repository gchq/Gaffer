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
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.core.exception.Status;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.rest.service.v2.example.ExamplesFactory;
import uk.gov.gchq.gaffer.user.User;
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser.createDefaultMapper;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;

public class OperationServiceV2 implements IOperationServiceV2 {
    private static final Logger LOGGER = LoggerFactory.getLogger(OperationServiceV2.class);
    public final ObjectMapper mapper = createDefaultMapper();

    @Inject
    private GraphFactory graphFactory;

    @Inject
    private UserFactory userFactory;

    @Inject
    private ExamplesFactory examplesFactory;

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
    public ChunkedOutput<String> executeChunked(final Operation operation) {
        return executeChunked(new OperationChain(operation));
    }

    @Override
    public Response operationDetails(final String className) throws InstantiationException, IllegalAccessException {
        try {
            return Response.ok(new OperationDetail(getOperationClass(className)))
                           .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                           .build();
        } catch (final ClassNotFoundException e) {
            LOGGER.info("Class: {} was not found on the classpath.", className, e);
            return Response.status(NOT_FOUND)
                           .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                           .build();
        }
    }

    @Override
    public Response executeOperation(final String className, final Operation operation) {
        if (className != operation.getClass().getCanonicalName()) {
            throw new BadRequestException("Class name does not match message body.");
        }

        return _executeRest(operation);
    }

    @Override
    public Response operationDocumentation(final String className) {
        throw new GafferRuntimeException("Operation documentation is not currently supported.", Status.NOT_IMPLEMENTED);
    }

    @Override
    public Response operationExample(final String className) throws InstantiationException, IllegalAccessException {
        try {
            return Response.ok(getExampleJson(getOperationClass(className)))
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

    private String getDocumentationLink() {
        throw new GafferRuntimeException("Operation documentation is not currently supported.", Status.NOT_IMPLEMENTED);
    }

    private Operation getExampleJson(final Class<? extends Operation> opClass) throws ClassNotFoundException,
            IllegalAccessException, InstantiationException {
        return examplesFactory.generateExample(opClass);
    }

    private Set<Class<? extends Operation>> getNextOperations(final Class<? extends Operation> opClass) {
        return graphFactory.getGraph().getNextOperations(opClass);
    }

    private Class<? extends Operation> getOperationClass(final String className) throws ClassNotFoundException {
        return Class.forName(className).asSubclass(Operation.class);
    }

    /**
     * POJO to store details for a single user defined field in an {@link uk.gov.gchq.gaffer.operation.Operation}.
     */
    private class OperationField {
        private final String name;
        private final boolean required;

        public OperationField(final String name, final boolean required) {
            this.name = name;
            this.required = required;
        }

        public String getName() {
            return name;
        }

        public boolean isRequired() {
            return required;
        }
    }

    /**
     * POJO to store details for a user specified {@link uk.gov.gchq.gaffer.operation.Operation}
     * class.
     */
    private class OperationDetail {
        private final String name;
        private final List<OperationField> fields;
        private final Set<Class<? extends Operation>> next;
        private final Operation exampleJson;

        public OperationDetail(final Class<? extends Operation> opClass) {
            this.name = opClass.getName();
            this.fields = getOperationFields(opClass);
            this.next = getNextOperations(opClass);
            try {
                this.exampleJson = OperationServiceV2.this.getExampleJson(opClass);
            } catch (final ClassNotFoundException | IllegalAccessException | InstantiationException e) {

                throw new GafferRuntimeException("Could not get operation details for class: " + name, e, Status.BAD_REQUEST);
            }
        }

        public String getName() {
            return name;
        }

        public List<OperationField> getFields() {
            return fields;
        }

        public Set<Class<? extends Operation>> getNext() {
            return next;
        }

        public Operation getExampleJson() {
            return exampleJson;
        }

        private List<OperationField> getOperationFields(final Class<? extends Operation> opClass) {
            return Arrays.asList(opClass.getDeclaredFields())
                         .stream()
                         .map(f -> {
                             boolean required = false;
                             final Required[] annotations = f.getAnnotationsByType(Required.class);

                             if (annotations != null && annotations.length > 0) {
                                 required = true;
                             }
                             return new OperationField(f.getName(), required);
                         })
                         .collect(Collectors.toList());
        }
    }
}
