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

package uk.gov.gchq.gaffer.rest.mapper;

import uk.gov.gchq.gaffer.core.exception.Error;
import uk.gov.gchq.gaffer.core.exception.ErrorFactory;
import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Jersey {@link javax.ws.rs.ext.ExceptionMapper} to be used to handle
 * {@link uk.gov.gchq.gaffer.core.exception.GafferCheckedException}s.
 */
@Provider
public class GafferCheckedExceptionMapper implements ExceptionMapper<GafferCheckedException> {

    @Override
    public Response toResponse(final GafferCheckedException gce) {
        final Error error = ErrorFactory.from(gce);

        return Response.status(error.getStatusCode())
                       .entity(error)
                       .build();
    }
}
