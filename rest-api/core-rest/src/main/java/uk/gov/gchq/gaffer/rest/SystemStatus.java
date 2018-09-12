/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import uk.gov.gchq.koryphe.Summary;

/**
 * POJO representing the Gaffer system status.
 */
public class SystemStatus {

    public static final SystemStatus UP = new SystemStatus(Status.UP);
    public static final SystemStatus DOWN = new SystemStatus(Status.DOWN);
    public static final SystemStatus UNKNOWN = new SystemStatus(Status.UNKNOWN);
    public static final SystemStatus OUT_OF_SERVICE = new SystemStatus(Status.OUT_OF_SERVICE);

    private final Status status;

    @JsonCreator
    public SystemStatus(@JsonProperty("status") final Status status) {
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final SystemStatus that = (SystemStatus) obj;

        return new EqualsBuilder()
                .append(status, that.status)
                .isEquals();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("status", status)
                .toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(status)
                .toHashCode();
    }

    /**
     * Enumerated type for the Gaffer system status.
     * This enum is compliant with the Spring Boot Actuator.
     */
    @Summary("Status of the system")
    public enum Status {

        UP("UP", "The system is working normally."),
        DOWN("DOWN", "The system is unavailable."),
        UNKNOWN("UNKNOWN", "The system status is unknown."),
        OUT_OF_SERVICE("OUT_OF_SERVICE", "The system is out of service.");

        private String description;

        private String code;

        Status(final String code, final String description) {
            this.code = code;
            this.description = description;
        }

        public String getCode() {
            return code;
        }

        public String getDescription() {
            return description;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("description", description)
                    .append("code", code)
                    .toString();
        }
    }
}
