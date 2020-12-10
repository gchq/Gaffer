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

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.ResponseHeader;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.rest.model.OperationDetail;

import java.util.Set;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;
import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER_DESCRIPTION;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.OK;

@RequestMapping("/graph/operations")
public interface IOperationController {

    // todo fix all the properties

    @GetMapping(
            path = "",
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(value = "Gets all operations supported by the store",
            notes = "This endpoint returns a list of the fully qualified classpaths, for all operations supported by the store.",
            produces = APPLICATION_JSON_VALUE,
            response = String.class,
            responseContainer = "list",
            responseHeaders = {
                    @ResponseHeader(name = GAFFER_MEDIA_TYPE_HEADER, description = GAFFER_MEDIA_TYPE_HEADER_DESCRIPTION)
            })
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK)})
    Set<Class<? extends Operation>> getOperations();

    @GetMapping(
            path = "/details",
            produces = APPLICATION_JSON_VALUE
    )
    @ApiResponse(
            message = "Returns the details of every operation supported by the store",
            code = 200,
            response = OperationDetail.class,
            responseContainer = "Set"
    )
    Set<OperationDetail> getAllOperationDetails();

    @RequestMapping(
            method = GET,
            value = "{className}",
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Gets details about the specified operation class",
            response = OperationDetail.class
    )
    OperationDetail getOperationDetails(final String className);

    @GetMapping(
            value = "{className}/next",
            produces = APPLICATION_JSON_VALUE
    )
    @ApiResponse(
            message = "Gets the operations that can be chained after a given operation",
            code = 200,
            response = String.class,
            responseContainer = "Set"
    )
    Set<Class<? extends Operation>> getNextOperations(final String className);

    @GetMapping(
            value = "{className}/example",
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Gets an example of an operation class",
            response = Operation.class
    )
    Operation getOperationExample(final String className);


    @PostMapping(
            path = "/execute",
            consumes = APPLICATION_JSON_VALUE,
            produces = { TEXT_PLAIN_VALUE, APPLICATION_JSON_VALUE }
    )
    @ApiOperation("Executes an operation against a Store")
    ResponseEntity<Object> execute(final Operation operation);
}
