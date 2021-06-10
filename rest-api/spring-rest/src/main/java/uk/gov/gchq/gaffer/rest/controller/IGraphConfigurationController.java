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

import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Map;
import java.util.Set;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;
import static org.springframework.web.bind.annotation.RequestMethod.GET;

@RequestMapping("/graph/config")
public interface IGraphConfigurationController {

    @RequestMapping(
            path = "/schema",
            method = GET,
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Gets the schema",
            response = Schema.class
    )
    Schema getSchema();

    @RequestMapping(
            path = "/description",
            method = GET,
            produces = TEXT_PLAIN_VALUE
    )
    @ApiOperation(
            value = "Gets the graph description",
            response = String.class
    )
    String getDescription();

    @RequestMapping(
            path = "/graphId",
            method = GET,
            produces = TEXT_PLAIN_VALUE
    )
    @ApiOperation(
            value = "Gets the graph Id",
            response = String.class
    )
    String getGraphId();

    @RequestMapping(
            path = "/filterFunctions",
            method = GET,
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Gets the available filter functions",
            response = Class.class,
            responseContainer = "Set"
    )
    Set<Class> getFilterFunctions();

    @RequestMapping(
            path = "/elementGenerators",
            method = GET,
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Gets the available element generators",
            response = Class.class,
            responseContainer = "Set"
    )
    Set<Class> getElementGenerators();

    @RequestMapping(
            path = "/filterFunctions/{inputClass}",
            produces = APPLICATION_JSON_VALUE,
            method = GET
    )
    @ApiOperation(
            value = "Gets the available filter functions for a given input class",
            response = Class.class,
            responseContainer = "Set"
    )
    Set<Class> getFilterFunctions(final String inputClass);

    @RequestMapping(
            path = "/objectGenerators",
            method = GET,
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Gets the available object generators",
            response = Class.class,
            responseContainer = "Set"
    )
    Set<Class> getObjectGenerators();

    @RequestMapping(
            path = "/serialisedFields/{className}",
            method = GET,
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Gets the serialised fields for a given class",
            response = String.class,
            responseContainer = "Set"
    )
    Set<String> getSerialisedFields(final String className);

    @RequestMapping(
            path = "/serialisedFields/{className}/classes",
            method = GET,
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Gets the serialised fields and their classes for a given class",
            response = String.class,
            responseContainer = "Map"
    )
    Map<String, String> getSerialisedFieldClasses(final String className);

    @RequestMapping(
            path = "/storeTraits",
            method = GET,
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Gets the store traits",
            response = StoreTrait.class,
            responseContainer = "Set"
    )
    Set<StoreTrait> getStoreTraits();

    @RequestMapping(
            path = "/transformFunctions",
            method = GET,
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Gets the available transform functions",
            response = Class.class,
            responseContainer = "Set"
    )
    Set<Class> getTransformFunctions();
}
