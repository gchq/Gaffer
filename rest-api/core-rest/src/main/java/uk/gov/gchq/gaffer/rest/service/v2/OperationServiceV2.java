/*
 * Copyright 2017-2018 Crown Copyright
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
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.core.exception.Error;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.core.exception.Status;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.rest.service.v2.example.ExamplesFactory;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;

import javax.inject.Inject;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser.createDefaultMapper;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.JOB_ID_HEADER;
import static uk.gov.gchq.gaffer.serialisation.util.JsonSerialisationUtil.getSerialisedFieldClasses;

/**
 * An implementation of {@link IOperationServiceV2}. By default it will use a singleton
 * {@link uk.gov.gchq.gaffer.graph.Graph} generated using the {@link uk.gov.gchq.gaffer.rest.factory.GraphFactory}.
 * All operations are simple delegated to the graph.
 * Pre and post operation hooks are available by extending this class and implementing preOperationHook and/or
 * postOperationHook.
 */
public class OperationServiceV2 implements IOperationServiceV2 {
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
        Set<Class<? extends Operation>> supportedOperations = graphFactory.getGraph().getSupportedOperations();
        List<OperationDetail> supportedClassesAsOperationDetail = new ArrayList<>();

        for (final Class<? extends Operation> supportedOperation : supportedOperations) {
            supportedClassesAsOperationDetail.add(new OperationDetail(supportedOperation));
        }

        return Response.ok(supportedClassesAsOperationDetail)
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .build();
    }

    @Override
    public Response execute(final Operation operation) {
        final Pair<Object, String> resultAndJobId = _execute(operation);
        return Response.ok(resultAndJobId.getFirst())
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .header(JOB_ID_HEADER, resultAndJobId.getSecond())
                .build();
    }

    @Override
    public ChunkedOutput<String> executeChunked(final Operation operation) {
        return executeChunkedChain(OperationChain.wrap(operation));
    }

    @SuppressFBWarnings
    @Override
    public ChunkedOutput<String> executeChunkedChain(final OperationChain opChain) {
        // Create chunked output instance
        final ChunkedOutput<String> output = new ChunkedOutput<>(String.class, "\r\n");

        // write chunks to the chunked output object
        new Thread(() -> {
            try {
                final Object result = _execute(opChain).getFirst();
                chunkResult(result, output);
            } finally {
                CloseableUtil.close(output);
                CloseableUtil.close(opChain);
            }
        }).start();

        return output;
    }

    @Override
    public Response operationDetails(final String className) throws InstantiationException, IllegalAccessException {
        try {
            final Class<? extends Operation> operationClass = getOperationClass(className);

            if (graphFactory.getGraph().getSupportedOperations().contains(operationClass)) {
                return Response.ok(new OperationDetail(operationClass))
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

    protected void preOperationHook(final OperationChain<?> opChain, final Context context) {
        // no action by default
    }

    protected void postOperationHook(final OperationChain<?> opChain, final Context context) {
        // no action by default
    }

    @SuppressWarnings("ThrowFromFinallyBlock")
    protected <O> Pair<O, String> _execute(final Operation operation) {

        OperationChain<O> opChain = (OperationChain<O>) OperationChain.wrap(operation);

        final Context context = userFactory.createContext();
        preOperationHook(opChain, context);

        O result;
        try {
            result = graphFactory.getGraph().execute(opChain, context);
        } catch (final OperationException e) {
            CloseableUtil.close(operation);
            if (null != e.getMessage()) {
                throw new RuntimeException("Error executing opChain: " + e.getMessage(), e);
            } else {
                throw new RuntimeException("Error executing opChain", e);
            }
        } finally {
            try {
                postOperationHook(opChain, context);
            } catch (final Exception e) {
                CloseableUtil.close(operation);
                throw e;
            }
        }

        return new Pair<>(result, context.getJobId());
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

    private Operation generateExampleJson(final Class<? extends Operation> opClass) throws IllegalAccessException, InstantiationException {
        return examplesFactory.generateExample(opClass);
    }

    private Set<Class<? extends Operation>> getNextOperations(final Class<? extends Operation> opClass) {
        return graphFactory.getGraph().getNextOperations(opClass);
    }

    private Class<? extends Operation> getOperationClass(final String className) throws ClassNotFoundException {
        return Class.forName(SimpleClassNameIdResolver.getClassName(className)).asSubclass(Operation.class);
    }

    private List<OperationField> getOperationFields(final Class<? extends Operation> opClass) {
        Map<String, String> fieldsToClassMap = getSerialisedFieldClasses(opClass.getName());
        List<OperationField> operationFields = new ArrayList<>();

        for (final String fieldString : fieldsToClassMap.keySet()) {
            boolean required = false;
            Field field = null;

            try {
                field = opClass.getDeclaredField(fieldString);
            } catch (final NoSuchFieldException e) {
                // Ignore, we will just assume it isn't required
            }

            if (null != field) {
                final Required[] annotations = field.getAnnotationsByType(Required.class);

                if (null != annotations && annotations.length > 0) {
                    required = true;
                }
            }
            operationFields.add(new OperationField(fieldString, required, fieldsToClassMap.get(fieldString)));
        }
        return operationFields;
    }

    private static String getOperationSummaryValue(final Class<? extends Operation> opClass) {
        final Summary summary = opClass.getAnnotation(Summary.class);
        return null != summary && null != summary.value() ? summary.value() : null;
    }

    /**
     * POJO to store details for a single user defined field in an {@link uk.gov.gchq.gaffer.operation.Operation}.
     */
    private class OperationField {
        private final String name;
        private String className;
        private final boolean required;

        OperationField(final String name, final boolean required, final String className) {
            this.name = name;
            this.required = required;
            this.className = className;
        }

        public String getName() {
            return name;
        }

        public boolean isRequired() {
            return required;
        }

        public String getClassName() {
            return className;
        }
    }

    /**
     * POJO to store details for a user specified {@link uk.gov.gchq.gaffer.operation.Operation}
     * class.
     */
    protected class OperationDetail {
        private final String name;
        private final String summary;
        private final List<OperationField> fields;
        private final Set<Class<? extends Operation>> next;
        private final Operation exampleJson;

        OperationDetail(final Class<? extends Operation> opClass) {
            this.name = opClass.getName();
            this.summary = getOperationSummaryValue(opClass);
            this.fields = getOperationFields(opClass);
            this.next = getNextOperations(opClass);
            try {
                this.exampleJson = generateExampleJson(opClass);
            } catch (final IllegalAccessException | InstantiationException e) {
                throw new GafferRuntimeException("Could not get operation details for class: " + name, e, Status.BAD_REQUEST);
            }
        }

        public String getName() {
            return name;
        }

        public String getSummary() {
            return summary;
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
    }
}
