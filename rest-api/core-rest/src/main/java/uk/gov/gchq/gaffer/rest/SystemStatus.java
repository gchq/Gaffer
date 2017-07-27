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


import io.swagger.annotations.ApiModelProperty;

public enum SystemStatus {

    UP("UP", "The system is working normally."),
    DOWN("DOWN", "The system is unavailable."),
    UNKNOWN("UNKNOWN", "The system status is unknown."),
    OUT_OF_SERVICE("OUT_OF_SERVICE", "The system is out of service.");

    @ApiModelProperty
    private String description;

    @ApiModelProperty
    private String code;

    private SystemStatus(final String code, final String description) {
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
