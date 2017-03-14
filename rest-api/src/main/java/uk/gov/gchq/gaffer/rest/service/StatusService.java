/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.service;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.core.exception.Status;
import uk.gov.gchq.gaffer.rest.SystemStatus;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * An <code>StatusService</code> has methods to check the status of the system
 */
@Path("/status")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/status", description = "Methods to check the status of the system.")
public class StatusService {

     @Inject
    private GraphFactory graphFactory;

    @Inject
    private UserFactory userFactory;

    @GET
    @ApiOperation(value = "Returns the status of the service", response = SystemStatus.class)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 500, message = "Something wrong in Server")})
    public SystemStatus status() {
        try {
            if (null != graphFactory.getGraph()) {
                return new SystemStatus("The system is working normally.");
            }
        } catch (final Exception e) {
            throw new GafferRuntimeException("Unable to create graph.", e, Status.IM_A_TEAPOT);
        }

        return new SystemStatus("Unable to create graph.");
    }
}
