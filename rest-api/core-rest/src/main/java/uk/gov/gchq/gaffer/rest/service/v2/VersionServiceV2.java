/*
 * Copyright 2023 Crown Copyright
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
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.ResponseHeader;

import uk.gov.gchq.gaffer.rest.SystemProperty;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER_DESCRIPTION;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.OK;

@Path("/graph/version")
@Api(value = "/version")
public class VersionServiceV2 {

    @GET
    @ApiOperation(
        value = "Returns the version of the gaffer graph",
        notes = "A simple way to check the version of the gaffer graph.",
        response = String.class,
        produces = TEXT_PLAIN,
            responseHeaders = {
                @ResponseHeader(name = GAFFER_MEDIA_TYPE_HEADER, description = GAFFER_MEDIA_TYPE_HEADER_DESCRIPTION)
            })
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK)})
    public Response getGafferVersion() {
        // Return the preloaded version string from the common-rest library
        return Response.ok(SystemProperty.GAFFER_VERSION_DEFAULT)
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .build();
    }

}
