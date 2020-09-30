/*
 * Copyright 2020 Crown Copyright
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

import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Map;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;
import static org.springframework.web.bind.annotation.RequestMethod.GET;

@RequestMapping("/properties")
public interface IPropertiesController {

    @RequestMapping(
            method = GET,
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Get all the Gaffer System properties"
    )
    Map<String, String> getProperties();

    @RequestMapping(
            value = "/{property}",
            method = GET,
            produces = { TEXT_PLAIN_VALUE, APPLICATION_JSON_VALUE }
    )
    @ApiOperation(
            value = "Gets the value of a provided property"
    )
    String getProperty(final String property);
}
