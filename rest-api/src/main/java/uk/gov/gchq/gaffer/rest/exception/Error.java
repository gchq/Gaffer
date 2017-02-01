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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.rest.exception.Error.ErrorBuilder;
import uk.gov.gchq.gaffer.rest.exception.serialisation.StatusTypeDeserialiser;
import uk.gov.gchq.gaffer.rest.exception.serialisation.StatusTypeSerialiser;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.StatusType;

/**
 * Simple serialisable POJO for containing details of REST errors.
 * An {@link uk.gov.gchq.gaffer.rest.exception.Error} object is typically
 * created automatically by a Jersey {@link javax.ws.rs.ext.ExceptionMapper} and
 * should not be created manually.
 */
@JsonDeserialize(builder = ErrorBuilder.class)
public class Error {

    private final int statusCode;
    private final StatusType status;
    private final String simpleMessage;
    private final String detailMessage;

    public Error(final ErrorBuilder builder) {
        this.statusCode = builder.statusCode;
        this.status = builder.status;
        this.simpleMessage = builder.simpleMessage;
        this.detailMessage = builder.detailMessage;
    }

    public int getStatusCode() {
        return statusCode;
    }

    @JsonSerialize(using = StatusTypeSerialiser.class)
    public StatusType getStatus() {
        return status;
    }

    public String getSimpleMessage() {
        return simpleMessage;
    }

    public String getDetailMessage() {
        return detailMessage;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        final Error error = (Error) o;

        return new EqualsBuilder()
                .append(statusCode, error.statusCode)
                .append(simpleMessage, error.simpleMessage)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(statusCode)
                .append(simpleMessage)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("statusCode", statusCode)
                .append("simpleMessage", simpleMessage)
                .toString();
    }

    @JsonPOJOBuilder(withPrefix = "")
    public final static class ErrorBuilder {
        private int statusCode;
        private StatusType status;
        private String simpleMessage;
        private String detailMessage;

        public ErrorBuilder() {
            // Empty
        }

        public ErrorBuilder statusCode(final int statusCode) {
            this.statusCode = statusCode;
            this.status = Response.Status.fromStatusCode(statusCode);
            return this;
        }

        @JsonDeserialize(using = StatusTypeDeserialiser.class)
        public ErrorBuilder status(final StatusType status) {
            this.status = status;
            this.statusCode = status.getStatusCode();
            return this;
        }

        public ErrorBuilder simpleMessage(final String simpleMessage) {
            this.simpleMessage = simpleMessage;
            return this;
        }

        public ErrorBuilder detailMessage(final String detailMessage) {
            this.detailMessage = detailMessage;
            return this;
        }

        public Error build() {
            return new Error(this);
        }
    }
}
