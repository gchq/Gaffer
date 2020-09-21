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
import org.springframework.http.RequestEntity;
import org.springframework.web.bind.annotation.RequestMapping;

import uk.gov.gchq.gaffer.data.generator.ElementGenerator;
import uk.gov.gchq.gaffer.data.generator.ObjectGenerator;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

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
    RequestEntity<Schema> getSchema();

    @RequestMapping(
            path = "/description",
            method = GET,
            produces = TEXT_PLAIN_VALUE
    )
    @ApiOperation(
            value = "Gets the graph description",
            response = String.class
    )
    RequestEntity<String> getDescription();

    @RequestMapping(
            path = "/filterFunctions",
            method = GET,
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Gets the available filter functions",
            response = Predicate.class,
            responseContainer = "Set"
    )
    RequestEntity<Set<? extends Predicate>> getFilterFunctions();

    @RequestMapping(
            path = "/elementGenerators",
            method = GET,
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Gets the available element generators",
            response = ElementGenerator.class,
            responseContainer = "Set"
    )
    RequestEntity<Set<? extends ElementGenerator>> getElementGenerators();

    @RequestMapping(
            path = "/filterFunctions/{inputClass}",
            method = GET,
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Gets the available filter functions for a given input class",
            response = Predicate.class,
            responseContainer = "Set"
    )
    RequestEntity<Set<? extends Predicate>> getFilterFunctions(final String inputClass);

    @RequestMapping(
            path = "/objectGenerators",
            method = GET,
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Gets the available object generators",
            response = ObjectGenerator.class,
            responseContainer = "Set"
    )
    RequestEntity<Set<? extends ObjectGenerator>> getObjectGenerators();

    @RequestMapping(
            path = "/serialisedFields/{className:.+}",
            method = GET,
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Gets the serialised fields for a given class",
            response = String.class,
            responseContainer = "Set"
    )
    RequestEntity<Set<String>> getSerialisedFields(final String className);

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
    RequestEntity<Map<String, String>> getSerialisedFieldClasses(final String className);

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
    RequestEntity<Set<StoreTrait>> getStoreTraits();

    @RequestMapping(
            path = "/transformFunctions",
            method = GET,
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Gets the available transform functions",
            response = Function.class,
            responseContainer = "Set"
    )
    RequestEntity<Set<? extends Function>> getTransformFunctions();
}
