/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.operation;

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.core.exception.Status;

import static uk.gov.gchq.gaffer.core.exception.Status.INTERNAL_SERVER_ERROR;

/**
 * An {@code OperationException} is thrown when an operation fails.
 */
public class OperationException extends GafferCheckedException {
    private static final long serialVersionUID = 3855512637690609379L;

    public OperationException(final String message) {
        super(message, INTERNAL_SERVER_ERROR);
    }

    public OperationException(final String message, final Status status) {
        super(message, status);
    }

    public OperationException(final String message, final Throwable e) {
        super(message, e, INTERNAL_SERVER_ERROR);
    }

    public OperationException(final String message, final Throwable e, final Status status) {
        super(message, e, status);
    }
}
