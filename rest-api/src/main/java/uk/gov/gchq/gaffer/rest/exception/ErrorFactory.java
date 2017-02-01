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

package uk.gov.gchq.gaffer.rest.exception;

import org.apache.commons.lang3.exception.ExceptionUtils;
import uk.gov.gchq.gaffer.store.StoreException;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static uk.gov.gchq.gaffer.rest.exception.Error.ErrorBuilder;

/**
 * Static utility class to standardise the instantiation of {@link Error}
 * objects.
 */
public class ErrorFactory {

    /**
     * Empty, private constructor to prevent instantiation.
     */
    private ErrorFactory() {
        // Empty
    }

    /**
     * Create an {@link uk.gov.gchq.gaffer.rest.exception.Error} object from a
     * {@link java.lang.Throwable}.
     *
     * @param throwable
     * @return
     */
    public static Error from(final Throwable throwable) {
        return new ErrorBuilder().status(INTERNAL_SERVER_ERROR)
                                 .simpleMessage(throwable.getMessage())
                                 .detailMessage(ExceptionUtils.getStackTrace(throwable))
                                 .build();
    }

    /**
     * Create an {@link uk.gov.gchq.gaffer.rest.exception.Error} object from an
     * Accumulo {@link uk.gov.gchq.gaffer.store.StoreException}.
     *
     * @param exception
     * @return
     */
    public static Error from(final StoreException ex) {
        return new ErrorBuilder().status(INTERNAL_SERVER_ERROR)
                                 .simpleMessage(ex.getMessage())
                                 .detailMessage(ExceptionUtils.getStackTrace(ex))
                                 .build();
    }
}
