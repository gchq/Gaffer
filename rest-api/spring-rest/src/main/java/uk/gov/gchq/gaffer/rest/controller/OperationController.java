/*
 * Copyright 2020-2021 Crown Copyright
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
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
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
import uk.gov.gchq.gaffer.rest.model.OperationDetail;
import uk.gov.gchq.gaffer.rest.service.v2.AbstractOperationService;

import java.io.IOException;
import java.util.Set;

import static uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser.createDefaultMapper;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.JOB_ID_HEADER;

@RestController
public class OperationController extends AbstractOperationService implements IOperationController {

    private final GraphFactory graphFactory;
    private final UserFactory userFactory;
    private final ExamplesFactory examplesFactory;

    public final ObjectMapper mapper = createDefaultMapper();

    @Autowired
    public OperationController(final GraphFactory graphFactory, final UserFactory userFactory, final ExamplesFactory examplesFactory) {
        this.graphFactory = graphFactory;
        this.userFactory = userFactory;
        this.examplesFactory = examplesFactory;
    }

    @Override
    public Set<Class<? extends Operation>> getOperations() {
        return getSupportedOperations();
    }

    @Override
    public Set<OperationDetail> getAllOperationDetails() {
        return getSupportedOperationDetails();
    }

    @Override
    public OperationDetail getOperationDetails(@PathVariable("className") @ApiParam(name = "className", value = "The Operation class") final String className) {
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

    @Override
    public Set<Class<? extends Operation>> getNextOperations(@PathVariable("className") @ApiParam(name = "className", value = "The Operation class") final String className) {
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

    @Override
    public Operation getOperationExample(@PathVariable("className") @ApiParam(name = "className", value = "The Operation class") final String className) {
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

    @Override
    public ResponseEntity<Object> execute(@RequestBody final Operation operation) {
        Pair<Object, String> resultAndJobId = _execute(operation, userFactory.createContext());
        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .header(JOB_ID_HEADER, resultAndJobId.getSecond())
                .body(resultAndJobId.getFirst());
    }

    @Override
    public ResponseEntity<StreamingResponseBody> executeChunked(@RequestBody final Operation operation) {
        StreamingResponseBody responseBody = response -> {
            try {
                Pair<Object, String> resultAndJobId = _execute(operation, userFactory.createContext());
                Object result = resultAndJobId.getFirst();
                if (result instanceof Iterable) {
                    final Iterable itr = (Iterable) result;
                    try {
                        for (final Object item : itr) {
                            String itemString = mapper.writeValueAsString(item) + "\r\n";
                            response.write(itemString.getBytes());
                            response.flush();
                        }
                    } catch (final IOException ioe) {
                        throw new GafferRuntimeException("Unable to serialise chunk: ", ioe, Status.INTERNAL_SERVER_ERROR);
                    } finally {
                        CloseableUtil.close(itr);
                    }
                } else {
                    try {
                        response.write(mapper.writeValueAsString(result).getBytes());
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
