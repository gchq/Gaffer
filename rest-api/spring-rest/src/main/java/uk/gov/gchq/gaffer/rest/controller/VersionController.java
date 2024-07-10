/*
 * Copyright 2023-2024 Crown Copyright
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
import io.swagger.v3.oas.annotations.tags.Tag;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import uk.gov.gchq.gaffer.rest.SystemProperty;

import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;

@Tag(name = "version")
@RestController
public class VersionController {

    /**
     * Rest endpoint for getting the Gaffer version of the graph.
     *
     * @return Version of the graph
     */
    @GetMapping(path = "/rest/graph/version", produces = TEXT_PLAIN_VALUE)
    @Operation(summary = "Retrieves the version of the Gaffer Graph")
    public String getGafferVersion() {
        // Return the preloaded version string from the common-rest library
        return SystemProperty.GAFFER_VERSION_DEFAULT;
    }
}
