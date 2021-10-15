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

package uk.gov.gchq.gaffer.rest.mapper;

import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import uk.gov.gchq.gaffer.commonutil.exception.UnauthorisedException;
import uk.gov.gchq.gaffer.core.exception.Error;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.core.exception.Status;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.gchq.gaffer.core.exception.Status.FORBIDDEN;
import static uk.gov.gchq.gaffer.core.exception.Status.NOT_FOUND;

class GafferExceptionMapperTest {
    @Test
    public void shouldPropagateForbiddenError() {
        // When
        GafferExceptionMapper gafferExceptionMapper = new GafferExceptionMapper();

        ResponseEntity<?> response = gafferExceptionMapper.handleUnauthorisedException(null, new UnauthorisedException("nah"));

        // Then
        assertEquals(FORBIDDEN.getStatusCode(), response.getStatusCode().value());
        assertEquals("nah", ((Error) response.getBody()).getSimpleMessage());
    }

    @Test
    public void shouldPropagateErrorFromGRE() {
        // Given
        GafferRuntimeException exception = new GafferRuntimeException("couldn't find the thing", Status.NOT_FOUND);

        // When
        GafferExceptionMapper gafferExceptionMapper = new GafferExceptionMapper();
        ResponseEntity<?> responseEntity = gafferExceptionMapper.handleGafferRuntimeException(null, exception);

        // Then
        assertEquals(NOT_FOUND.getStatusCode(), responseEntity.getStatusCode().value());
        assertEquals("couldn't find the thing", ((Error) responseEntity.getBody()).getSimpleMessage());
    }
}
