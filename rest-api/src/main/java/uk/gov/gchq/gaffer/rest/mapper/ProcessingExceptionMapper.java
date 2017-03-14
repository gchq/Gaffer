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

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import static uk.gov.gchq.gaffer.core.exception.Status.INTERNAL_SERVER_ERROR;

/**
 * Jersey {@link javax.ws.rs.ext.ExceptionMapper} used to handle internal
 * {@link javax.ws.rs.ProcessingException}s thrown by the Jersey framework.
 */
@Provider
public class ProcessingExceptionMapper implements ExceptionMapper<ProcessingException> {

    @Override
    public Response toResponse(final ProcessingException pe) {
        throw new GafferRuntimeException("Rethrown ProcessingException", pe, INTERNAL_SERVER_ERROR);
    }
}
