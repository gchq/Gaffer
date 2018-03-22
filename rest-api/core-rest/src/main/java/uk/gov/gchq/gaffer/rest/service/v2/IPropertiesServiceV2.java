/*
 * Copyright 2016-2018 Crown Copyright
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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.ResponseHeader;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import java.util.Map;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER_DESCRIPTION;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.INTERNAL_SERVER_ERROR;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.OK;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.PROPERTY_NOT_FOUND;

/**
 * An {@code IPropertiesServiceV2} has methods to GET a list of configured system properties
 */
@Path("/properties")
@Produces(APPLICATION_JSON)
@Consumes(APPLICATION_JSON)
@Api(value = "properties")
public interface IPropertiesServiceV2 {

    @GET
    @Path("/")
    @ApiOperation(value = "Gets all available properties",
            notes = "Retrieves all properties associated with the application, " +
                    "eg. the app title, or the logo hyperlink.",
            response = Map.class,
            produces = APPLICATION_JSON,
            responseHeaders = {
                    @ResponseHeader(name = GAFFER_MEDIA_TYPE_HEADER, description = GAFFER_MEDIA_TYPE_HEADER_DESCRIPTION)
            })
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK), @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getProperties();

    @GET
    @Path("/{propertyName}")
    @Produces({TEXT_PLAIN, APPLICATION_JSON})
    @ApiOperation(value = "Gets the property value for the specified property name",
            notes = "Retrieves the value of the provided system property.",
            response = String.class,
            produces = TEXT_PLAIN,
            responseHeaders = {
                    @ResponseHeader(name = GAFFER_MEDIA_TYPE_HEADER, description = GAFFER_MEDIA_TYPE_HEADER_DESCRIPTION)
            })
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 404, message = PROPERTY_NOT_FOUND),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getProperty(@ApiParam(value = "The property name for which the value should be retrieved") @PathParam("propertyName") final String propertyName);

}
