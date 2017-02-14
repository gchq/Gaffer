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

import javax.ws.rs.core.Response.StatusType;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

public class GafferRuntimeException extends RuntimeException {

    private StatusType status = INTERNAL_SERVER_ERROR;

    public GafferRuntimeException(final String message, final StatusType status) {
        super(message);
        this.status = status;
    }

    public GafferRuntimeException(final String message, final Throwable cause, final StatusType status) {
        super(message, cause);
        this.status = status;
    }

    public StatusType getStatus() {
        return status;
    }

    public void setStatus(final StatusType status) {
        this.status = status;
    }
}
