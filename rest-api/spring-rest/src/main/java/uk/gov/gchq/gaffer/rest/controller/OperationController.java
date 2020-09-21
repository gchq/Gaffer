/*
 * Copyright 2020 Crown Copyright
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

import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.core.exception.Status;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.rest.factory.ExamplesFactory;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.rest.model.OperationDetail;
import uk.gov.gchq.gaffer.rest.service.v2.AbstractOperationService;

import java.util.Set;

import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.JOB_ID_HEADER;

@RestController
public class OperationController extends AbstractOperationService implements IOperationController {

    private GraphFactory graphFactory;
    private UserFactory userFactory;
    private ExamplesFactory examplesFactory;

    @Autowired
    public void setGraphFactory(final GraphFactory graphFactory) {
        this.graphFactory = graphFactory;
    }

    @Autowired
    public void setUserFactory(final UserFactory userFactory) {
        this.userFactory = userFactory;
    }

    @Autowired
    public void setExamplesFactory(final ExamplesFactory examplesFactory) {
        this.examplesFactory = examplesFactory;
    }

    @Override
    public ResponseEntity<Set<Class<? extends Operation>>> getOperations() {
        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .body(getSupportedOperations());
    }

    @Override
    public ResponseEntity<Set<OperationDetail>> getAllOperationDetails() {
        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .body(getSupportedOperationDetails());
    }

    @Override
    public ResponseEntity<OperationDetail> getOperationDetails(@PathVariable("className") @ApiParam(name = "className", value = "The Operation class") final String className) {
        try {
            final Class<? extends Operation> operationClass = getOperationClass(className);

            if (graphFactory.getGraph().getSupportedOperations().contains(operationClass)) {
                return ResponseEntity.ok()
                        .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                        .body(new OperationDetail(operationClass, getNextOperations(operationClass), generateExampleJson(operationClass)));
            } else {
                throw new GafferRuntimeException("Class: " + className + " is not supported by the current store.");
            }
        } catch (final ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new GafferRuntimeException("Class: " + className + " was not found on the classpath.", e);
        }
    }

    @Override
    public ResponseEntity<Set<Class<? extends Operation>>> getNextOperations(@PathVariable("className") @ApiParam(name = "className", value = "The Operation class") final String className) {
        Class<? extends Operation> opClass;
        try {
            opClass = getOperationClass(className);
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException("Operation class was not found: " + className, e);
        } catch (final ClassCastException e) {
            throw new IllegalArgumentException(className + " does not extend Operation", e);
        }

        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .body(getNextOperations(opClass));
    }

    @Override
    public ResponseEntity<Operation> getOperationExample(@PathVariable("className") @ApiParam(name = "className", value = "The Operation class") final String className) {
        Class<? extends Operation> operationClass;
        try {
            operationClass = getOperationClass(className);
        } catch (final ClassNotFoundException e) {
            throw new GafferRuntimeException("Unable to find operation class " + className + " on the classpath", e, Status.NOT_FOUND);
        }
        try {
            return ResponseEntity.ok()
                    .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                    .body(generateExampleJson(operationClass));
        } catch (final InstantiationException | IllegalAccessException e) {
            throw new GafferRuntimeException("Unable to create example for class " + className, e, Status.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public ResponseEntity<Object> execute(@RequestBody final Operation operation) {
        Pair<Object, String> resultAndGraphId = _execute(operation, userFactory.createContext());
        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .header(JOB_ID_HEADER, resultAndGraphId.getSecond())
                .body(resultAndGraphId.getFirst());
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
