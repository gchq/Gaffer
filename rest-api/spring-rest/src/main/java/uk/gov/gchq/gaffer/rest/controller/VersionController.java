/*
 * Copyright 2023 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.controller;

import io.swagger.v3.oas.annotations.Operation;
import org.springframework.web.bind.annotation.RestController;

import uk.gov.gchq.gaffer.rest.SystemProperty;

import org.springframework.web.bind.annotation.GetMapping;

@RestController
public class VersionController {

    @GetMapping(path = "/graph/version")
    @Operation(summary = "Retrieves the version of the Gaffer Graph")
    public String getGafferVersion() {
        // Return the preloaded version string from the common-rest library
        return SystemProperty.GAFFER_VERSION_DEFAULT;
    }
}
