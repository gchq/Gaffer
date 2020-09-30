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

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import uk.gov.gchq.gaffer.commonutil.exception.UnauthorisedException;
import uk.gov.gchq.gaffer.core.exception.Error;
import uk.gov.gchq.gaffer.core.exception.ErrorFactory;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.core.exception.GafferWrappedErrorRuntimeException;

import javax.servlet.http.HttpServletRequest;

@ControllerAdvice
public class GafferExceptionMapper extends ResponseEntityExceptionHandler {

    @ExceptionHandler(GafferRuntimeException.class)
    @ResponseBody
    public ResponseEntity<?> handleGafferRuntimeException(final HttpServletRequest request, final GafferRuntimeException gre) {
        final Error error = ErrorFactory.from(gre);

        return ResponseEntity.status(error.getStatusCode())
                .body(error);
    }

    @ExceptionHandler(UnauthorisedException.class)
    @ResponseBody
    public ResponseEntity<?> handleUnauthorisedException(final HttpServletRequest request, final UnauthorisedException e) {
        final Error error = ErrorFactory.from(e);

        return ResponseEntity.status(error.getStatusCode())
                .body(error);
    }

    @ExceptionHandler(GafferWrappedErrorRuntimeException.class)
    @ResponseBody
    public ResponseEntity<?> handleUnauthorisedException(final HttpServletRequest request, final GafferWrappedErrorRuntimeException e) {
        final Error error = ErrorFactory.from(e);

        return ResponseEntity.status(error.getStatusCode())
                .body(error);
    }

    @ExceptionHandler(Exception.class)
    @ResponseBody
    public ResponseEntity<?> handleAllOtherTypesOfException(final HttpServletRequest request, final Exception e) {
        final Error error = ErrorFactory.from(e);

        return ResponseEntity.status(error.getStatusCode())
                .body(error);
    }



}
