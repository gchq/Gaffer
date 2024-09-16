/*
 * Copyright 2020-2024 Crown Copyright
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
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.core.exception.Status;
import uk.gov.gchq.gaffer.rest.PropertiesUtil;

import java.util.Map;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;

@RestController
@Tag(name = "properties")
@RequestMapping("/rest/properties")
public class PropertiesController {

    @GetMapping(produces = APPLICATION_JSON_VALUE)
    @Operation(summary = "Get all the Gaffer System properties")
    public Map<String, String> getProperties() {
        return PropertiesUtil.getProperties();
    }

    @GetMapping(value = "/{property}", produces = { TEXT_PLAIN_VALUE, APPLICATION_JSON_VALUE })
    @Operation(summary = "Gets the value of a provided property")
    public String getProperty(@PathVariable("property") final String property) {
        final String resolvedPropertyValue = PropertiesUtil.getProperty(property);
        if (resolvedPropertyValue != null) {
            return resolvedPropertyValue;
        }

        throw new GafferRuntimeException("Property: " + property + " could not be found.", Status.NOT_FOUND);
    }
}
