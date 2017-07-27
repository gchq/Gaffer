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

package uk.gov.gchq.gaffer.rest;

public class SystemStatus {

    public static SystemStatus UP = new SystemStatus(Status.UP);
    public static SystemStatus DOWN = new SystemStatus(Status.DOWN);
    public static SystemStatus UNKNOWN = new SystemStatus(Status.UNKNOWN);
    public static SystemStatus OUT_OF_SERVICE = new SystemStatus(Status.OUT_OF_SERVICE);

    private final Status status;

    public SystemStatus(final Status status) {
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }

    public enum Status {

        UP("UP", "The system is working normally."),
        DOWN("DOWN", "The system is unavailable."),
        UNKNOWN("UNKNOWN", "The system status is unknown."),
        OUT_OF_SERVICE("OUT_OF_SERVICE", "The system is out of service.");

        private String description;

        private String code;

        private Status(final String code, final String description) {
            this.code = code;
            this.description = description;
        }

        public String getCode() {
            return code;
        }

        public String getDescription() {
            return description;
        }
    }
}
