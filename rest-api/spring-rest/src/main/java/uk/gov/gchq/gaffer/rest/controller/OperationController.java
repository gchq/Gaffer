/*
 * Copyright 2020-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.tags.Tag;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.core.exception.Status;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.rest.factory.ExamplesFactory;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.rest.factory.spring.AbstractUserFactory;
import uk.gov.gchq.gaffer.rest.model.OperationDetail;
import uk.gov.gchq.gaffer.rest.service.v2.AbstractOperationService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import static uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser.createDefaultMapper;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.JOB_ID_HEADER;

@RestController
@Tag(name = "operations")
@RequestMapping("/rest/graph/operations")
public class OperationController extends AbstractOperationService {

    private final GraphFactory graphFactory;
    private final AbstractUserFactory userFactory;
    private final ExamplesFactory examplesFactory;

    public final ObjectMapper mapper = createDefaultMapper();

    @Autowired
    public OperationController(final GraphFactory graphFactory, final AbstractUserFactory userFactory, final ExamplesFactory examplesFactory) {
        this.graphFactory = graphFactory;
        this.userFactory = userFactory;
        this.examplesFactory = examplesFactory;
    }

    @GetMapping(produces = APPLICATION_JSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(summary = "Retrieves a list of supported operations")
    public Set<Class<? extends Operation>> getOperations() {
        return getSupportedOperations();
    }

    @GetMapping(path = "/all", produces = APPLICATION_JSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(summary = "Retrieves a list of all operations")
    public Set<Class<? extends Operation>> getOperationsIncludingUnsupported() {
        return getSupportedOperations(true);
    }

    @GetMapping(path = "/details", produces = APPLICATION_JSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(summary = "Returns the details of every operation supported by the store")
    public Set<OperationDetail> getAllOperationDetails() {
        return getSupportedOperationDetails();
    }

    @GetMapping(path = "/details/all", produces = APPLICATION_JSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(summary = "Returns the details of every operation")
    public Set<OperationDetail> getAllOperationDetailsIncludingUnsupported() {
        return getSupportedOperationDetails(true);
    }

    @GetMapping(value = "{className}", produces = APPLICATION_JSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(summary = "Gets details about the specified operation class")
    public OperationDetail getOperationDetails(@PathVariable("className") @Parameter(name = "className", description = "The Operation class") final String className) {
        try {
            final Class<? extends Operation> operationClass = getOperationClass(className);

            if (graphFactory.getGraph().getSupportedOperations().contains(operationClass)) {
                return new OperationDetail(operationClass, getNextOperations(operationClass), generateExampleJson(operationClass));
            } else {
                throw new GafferRuntimeException("Class: " + className + " is not supported by the current store.");
            }
        } catch (final ClassNotFoundException e) {
            throw new GafferRuntimeException("Class: " + className + " was not found on the classpath.", e, Status.NOT_FOUND);
        } catch (final ClassCastException e) {
            throw new GafferRuntimeException(className + " does not extend Operation", e, Status.BAD_REQUEST);
        } catch (final  IllegalAccessException | InstantiationException e) {
            throw new GafferRuntimeException("Unable to instantiate " + className, e);
        }
    }

    @GetMapping(value = "{className}/next", produces = APPLICATION_JSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(summary = "Gets the operations that can be chained after a given operation")
    public Set<Class<? extends Operation>> getNextOperations(@PathVariable("className") @Parameter(name = "className", description = "The Operation class") final String className) {
        Class<? extends Operation> opClass;
        try {
            opClass = getOperationClass(className);
        } catch (final ClassNotFoundException e) {
            throw new GafferRuntimeException("Operation class was not found: " + className, e, Status.NOT_FOUND);
        } catch (final ClassCastException e) {
            throw new GafferRuntimeException(className + " does not extend Operation", e, Status.BAD_REQUEST);
        }

        return getNextOperations(opClass);
    }

    @GetMapping(value = "{className}/example", produces = APPLICATION_JSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(summary = "Gets an example of an operation class")
    public Operation getOperationExample(@PathVariable("className") @Parameter(name = "className", description = "The Operation class") final String className) {
        Class<? extends Operation> operationClass;
        try {
            operationClass = getOperationClass(className);
        } catch (final ClassNotFoundException e) {
            throw new GafferRuntimeException("Class: " + className + " was not found on the classpath.", e, Status.NOT_FOUND);
        } catch (final ClassCastException e) {
            throw new GafferRuntimeException(className + " does not extend Operation", e, Status.BAD_REQUEST);
        }
        try {
            return generateExampleJson(operationClass);
        } catch (final InstantiationException | IllegalAccessException e) {
            throw new GafferRuntimeException("Unable to create example for class " + className, e, Status.INTERNAL_SERVER_ERROR);
        }
    }

    private static final String GET_ALL_ELEMENTS =
            "{\n" +
            "  \"class\": \"GetAllElements\"\n" +
            "}";
    private static final String GET_ELEMENTS =
            "{\n" +
            "  \"class\" : \"GetElements\",\n" +
            "  \"input\" : [ {\n" +
            "    \"class\" : \"EntitySeed\",\n" +
            "    \"vertex\" : \"1\"\n" +
            "  } ]\n" +
            "}";
    private static final String GET_ELEMENTS_WITH_VIEW =
            "{\n" +
            "  \"class\" : \"GetElements\",\n" +
            "  \"input\" : [ {\n" +
            "    \"class\" : \"EntitySeed\",\n" +
            "    \"vertex\" : \"1\"\n" +
            "  } ],\n" +
            "  \"view\" : {\n" +
            "    \"edges\" : {\n" +
            "      \"created\" : {}\n" +
            "    }\n" +
            "  }\n" +
            "}";
    private static final String OPERATION_CHAIN =
            "{\n" +
            "  \"class\":\"OperationChain\",\n" +
            "  \"operations\":[\n" +
            "    {\n" +
            "      \"class\":\"GetAdjacentIds\",\n" +
            "      \"input\":[\n" +
            "        {\n" +
            "          \"class\":\"EntitySeed\",\n" +
            "          \"vertex\":\"1\"\n" +
            "        }\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"class\":\"GetElements\",\n" +
            "      \"view\":{\n" +
            "        \"allEntities\":true\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    @PostMapping(path = "/execute", consumes = APPLICATION_JSON_VALUE, produces = APPLICATION_JSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(summary = "Executes an operation against a Store")
    public ResponseEntity<Object> execute(@RequestHeader final HttpHeaders httpHeaders,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(required = true, content = @Content(examples = {
                    @ExampleObject(name = "Get all elements", description = "Get all elements", value = GET_ALL_ELEMENTS),
                    @ExampleObject(name = "Get elements with seed", description = "Get elements seeded with Vertex 1", value = GET_ELEMENTS),
                    @ExampleObject(name = "Get edges with label", description = "Get all 'created' edges from Vertex 1", value = GET_ELEMENTS_WITH_VIEW),
                    @ExampleObject(name = "Get adjacent vertices", description = "Get all vertices adjacent to Vertex 1", value = OPERATION_CHAIN),
            }))
            @RequestBody final Operation operation) {
        userFactory.setHttpHeaders(httpHeaders);
        final Pair<Object, String> resultAndJobId = _execute(operation, userFactory.createContext());
        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .header(JOB_ID_HEADER, resultAndJobId.getSecond())
                .body(resultAndJobId.getFirst());
    }

    @PostMapping(path = "/execute/chunked", consumes = APPLICATION_JSON_VALUE, produces = APPLICATION_JSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(summary = "Executes an operation against a Store, returning a chunked output")
    @SuppressWarnings("PMD.UseTryWithResources")
    public ResponseEntity<StreamingResponseBody> executeChunked(
            @RequestHeader final HttpHeaders httpHeaders,
            @RequestBody final Operation operation) {
        userFactory.setHttpHeaders(httpHeaders);
        final StreamingResponseBody responseBody = response -> {
            try {
                final Pair<Object, String> resultAndJobId = _execute(operation, userFactory.createContext());
                final Object result = resultAndJobId.getFirst();
                if (result instanceof Iterable) {
                    final Iterable itr = (Iterable) result;
                    try {
                        for (final Object item : itr) {
                            final String itemString = mapper.writeValueAsString(item) + "\r\n";
                            response.write(itemString.getBytes(StandardCharsets.UTF_8));
                            response.flush();
                        }
                    } catch (final IOException ioe) {
                        throw new GafferRuntimeException("Unable to serialise chunk: ", ioe, Status.INTERNAL_SERVER_ERROR);
                    } finally {
                        CloseableUtil.close(itr);
                    }
                } else {
                    try {
                        response.write(mapper.writeValueAsString(result).getBytes(StandardCharsets.UTF_8));
                        response.flush();
                    } catch (final IOException ioe) {
                        throw new GafferRuntimeException("Unable to serialise chunk: ", ioe, Status.INTERNAL_SERVER_ERROR);
                    }
                }
            } catch (final Exception e) {
                throw new GafferRuntimeException("Unable to create chunk: ", e, Status.INTERNAL_SERVER_ERROR);
            } finally {
                CloseableUtil.close(operation);
            }
        };

        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .contentType(MediaType.APPLICATION_JSON)
                .body(responseBody);
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

}
