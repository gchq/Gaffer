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
package uk.gov.gchq.gaffer.rest.mapper;

import org.apache.commons.lang3.exception.ExceptionUtils;

import uk.gov.gchq.gaffer.core.exception.Error;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;

/**
 * Jersey {@link javax.ws.rs.ext.ExceptionMapper} used to handle internal
 * {@link javax.ws.rs.WebApplicationException}s thrown by the Jersey framework.
 */
@Provider
public class WebApplicationExceptionMapper implements ExceptionMapper<WebApplicationException> {

    @Override
    public Response toResponse(final WebApplicationException ex) {

        final Error error = new Error.ErrorBuilder()
                .statusCode(ex.getResponse().getStatus())
                .simpleMessage(ex.getMessage())
                .detailMessage(ExceptionUtils.getStackTrace(ex))
                .build();

        return Response.status(ex.getResponse().getStatus())
                       .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                       .entity(error)
                       .build();
    }
}
