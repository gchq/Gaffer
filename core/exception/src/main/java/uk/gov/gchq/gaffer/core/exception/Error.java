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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.core.exception.Error.ErrorBuilder;
import uk.gov.gchq.gaffer.core.exception.serialisation.StatusDeserialiser;
import uk.gov.gchq.gaffer.core.exception.serialisation.StatusSerialiser;

/**
 * Simple serialisable POJO for containing details of errors.
 * An {@link uk.gov.gchq.gaffer.core.exception.Error} object is typically
 * created automatically by a Jersey ExceptionMapper and should not be created
 * manually.
 */
@JsonDeserialize(builder = ErrorBuilder.class)
public final class Error {
    public static final String DEBUG = "gaffer.error-mode.debug";
    public static final String DEBUG_DEFAULT = String.valueOf(false);
    private final int statusCode;
    private final Status status;
    private final String simpleMessage;
    private final String detailMessage;

    private Error(final ErrorBuilder builder) {
        this.statusCode = builder.statusCode;
        this.status = builder.status;
        this.simpleMessage = builder.simpleMessage;
        this.detailMessage = builder.detailMessage;
    }

    public int getStatusCode() {
        return statusCode;
    }

    @JsonSerialize(using = StatusSerialiser.class)
    public Status getStatus() {
        return status;
    }

    public String getSimpleMessage() {
        return simpleMessage;
    }

    public String getDetailMessage() {
        return detailMessage;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        final Error error = (Error) obj;

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
    public static final class ErrorBuilder {
        private static final Logger LOGGER = LoggerFactory.getLogger(ErrorBuilder.class);
        private static boolean isDebug = checkDebugMode();
        private int statusCode;
        private Status status;
        private String simpleMessage;
        private String detailMessage;

        private static boolean checkDebugMode() {
            try {
                isDebug = Boolean.valueOf(System.getProperty(DEBUG, DEBUG_DEFAULT).trim());
                if (isDebug) {
                    LOGGER.debug("Detailed error message has been enabled in SystemProperties");
                }
            } catch (Exception e) {
                LOGGER.error("Defaulting Debug flag. Could not assign from System Properties: {}", e.getMessage());
                isDebug = Boolean.valueOf(DEBUG_DEFAULT);
            }

            return isDebug;
        }

        public static void updateDebugMode() {
            isDebug = checkDebugMode();
        }

        public ErrorBuilder() {
            // Empty
        }

        public ErrorBuilder statusCode(final int statusCode) {
            this.statusCode = statusCode;
            this.status = Status.fromStatusCode(statusCode);
            return this;
        }

        @JsonDeserialize(using = StatusDeserialiser.class)
        public ErrorBuilder status(final Status status) {
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
            return new Error(isDebug ? this : this.detailMessage(null));
        }

        @Override
        public String toString() {
            final ToStringBuilder sb = new ToStringBuilder(this);
            sb.append("simpleMessage", simpleMessage);
            sb.append("detailMessage", detailMessage);
            sb.append("statusCode", statusCode);
            sb.append("status", status);
            return sb.build();
        }
    }
}
