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

package uk.gov.gchq.gaffer.core.exception;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.core.exception.Error.ErrorBuilder;

/**
 * Static utility class to standardise the instantiation of {@link uk.gov.gchq.gaffer.core.exception.Error}
 * objects.
 */
public final class ErrorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorFactory.class);

    /**
     * Empty, private constructor to prevent instantiation.
     */
    private ErrorFactory() {
        // Empty
    }

    /**
     * Create an {@link uk.gov.gchq.gaffer.core.exception.Error} object from a
     * {@link uk.gov.gchq.gaffer.core.exception.GafferCheckedException}.
     *
     * @param gex the exception object
     * @return a newly constructed {@link uk.gov.gchq.gaffer.core.exception.Error}
     */
    public static Error from(final GafferCheckedException gex) {
        LOGGER.error("Error: {}", gex.getMessage(), gex);
        return new ErrorBuilder()
                .status(gex.getStatus())
                .simpleMessage(gex.getMessage())
                .detailMessage(ExceptionUtils.getStackTrace(gex))
                .build();
    }

    /**
     * Create an {@link uk.gov.gchq.gaffer.core.exception.Error} object from a
     * {@link uk.gov.gchq.gaffer.core.exception.GafferRuntimeException}.
     *
     * @param gex the exception object
     * @return a newly constructed {@link uk.gov.gchq.gaffer.core.exception.Error}
     */
    public static Error from(final GafferRuntimeException gex) {
        LOGGER.error("Error: {}", gex.getMessage(), gex);
        return new ErrorBuilder()
                .status(gex.getStatus())
                .simpleMessage(gex.getMessage())
                .detailMessage(ExceptionUtils.getStackTrace(gex))
                .build();
    }

    /**
     * Create an {@link uk.gov.gchq.gaffer.core.exception.Error} object from an
     * {@link java.lang.Exception}.
     *
     * @param ex the exception object
     * @return a newly constructed {@link uk.gov.gchq.gaffer.core.exception.Error}
     */
    public static Error from(final Exception ex) {
        LOGGER.error("Error: {}", ex.getMessage(), ex);
        return new ErrorBuilder()
                .status(Status.INTERNAL_SERVER_ERROR)
                .simpleMessage(ex.getMessage())
                .detailMessage(ExceptionUtils.getStackTrace(ex))
                .build();
    }
}
