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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.data.generator.ElementGenerator;
import uk.gov.gchq.gaffer.data.generator.ObjectGenerator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.serialisation.util.JsonSerialisationUtil;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;
import uk.gov.gchq.koryphe.signature.Signature;
import uk.gov.gchq.koryphe.util.ReflectionUtil;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;

@RestController
@Tag(name = "config")
@RequestMapping("/rest/graph/config")
public class GraphConfigurationController {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphConfigurationController.class);

    private final GraphFactory graphFactory;

    @Autowired
    public GraphConfigurationController(final GraphFactory graphFactory) {
        this.graphFactory = graphFactory;
    }

    @GetMapping(path = "/schema", produces = APPLICATION_JSON_VALUE)
    @Operation(summary = "Gets the schema")
    public Schema getSchema() {
        return graphFactory.getGraph().getSchema();
    }

    @GetMapping(path = "/description", produces = TEXT_PLAIN_VALUE)
    @Operation(summary = "Gets the graph description")
    public String getDescription() {
        return graphFactory.getGraph().getDescription();
    }

    @GetMapping(path = "/graphId", produces = TEXT_PLAIN_VALUE)
    @Operation(summary = "Gets the graph Id")
    public String getGraphId() {
        return graphFactory.getGraph().getGraphId();
    }

    @GetMapping(path = "/graphCreatedTime", produces = TEXT_PLAIN_VALUE)
    @Operation(summary = "Gets the creation time for the store associated with this REST API")
    public String getGraphCreatedTime() {
        return graphFactory.getGraph().getCreatedTime();
    }

    @GetMapping(path = "/filterFunctions", produces = APPLICATION_JSON_VALUE)
    @Operation(summary = "Gets the available filter functions")
    public Set<Class> getFilterFunctions() {
        return ReflectionUtil.getSubTypes(Predicate.class);
    }

    @GetMapping(path = "/elementGenerators", produces = APPLICATION_JSON_VALUE)
    @Operation(summary = "Gets the available element generators")
    public Set<Class> getElementGenerators() {
        return ReflectionUtil.getSubTypes(ElementGenerator.class);
    }

    @GetMapping(path = "/filterFunctions/{inputClass}", produces = APPLICATION_JSON_VALUE)
    @Operation(summary = "Gets the available filter functions for a given input class")
    public Set<Class> getFilterFunctions(@PathVariable("inputClass") final String inputClass) {
        if (StringUtils.isEmpty(inputClass)) {
            return getFilterFunctions();
        }

        final Class<?> clazz;
        try {
            clazz = Class.forName(SimpleClassNameIdResolver.getClassName(inputClass));
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find input class: " + inputClass, e);
        }

        final Set<Class> applicablePredicates = new HashSet<>();

        for (final Class predicateClass : ReflectionUtil.getSubTypes(Predicate.class)) {
            Predicate predicate;
            try {
                predicate = (Predicate) predicateClass.newInstance();
            } catch (final IllegalAccessException | InstantiationException e) {
                LOGGER.warn("Failed to create new instance of {}", predicateClass, e);
                LOGGER.warn("Skipping");
                continue;
            }

            Signature inputSignature = Signature.getInputSignature(predicate);
            if (null == inputSignature.getNumClasses()
                    || (1 == inputSignature.getNumClasses() &&
                    (Signature.UnknownGenericType.class.isAssignableFrom(inputSignature
                            .getClasses()[0])
                            || inputSignature.getClasses()[0].isAssignableFrom(clazz)))) {
                applicablePredicates.add(predicateClass);
            }
        }

        return applicablePredicates;
    }

    @GetMapping(path = "/objectGenerators", produces = APPLICATION_JSON_VALUE)
    @Operation(summary = "Gets the available object generators")
    public Set<Class> getObjectGenerators() {
        return ReflectionUtil.getSubTypes(ObjectGenerator.class);
    }

    @GetMapping(path = "/serialisedFields/{className}", produces = APPLICATION_JSON_VALUE)
    @Operation(summary = "Gets the serialised fields for a given class")
    public Set<String> getSerialisedFields(@PathVariable("className") final String className) {
        return JsonSerialisationUtil.getSerialisedFieldClasses(className).keySet();
    }

    @GetMapping(path = "/serialisedFields/{className}/classes", produces = APPLICATION_JSON_VALUE)
    @Operation(summary = "Gets the serialised fields and their classes for a given class")
    public Map<String, String> getSerialisedFieldClasses(@PathVariable("className") final String className) {
        return JsonSerialisationUtil.getSerialisedFieldClasses(className);
    }

    @GetMapping(path = "/storeType", produces = TEXT_PLAIN_VALUE)
    @Operation(summary = "Gets the store type")
    public String getStoreType() {
        return graphFactory.getGraph().getStoreProperties().getStoreClass();
    }

    @GetMapping(path = "/storeTraits", produces = APPLICATION_JSON_VALUE)
    @Operation(summary = "Gets all store traits supported by this store")
    public Set<StoreTrait> getStoreTraits() {
        try {
            return graphFactory.getGraph().execute(new GetTraits.Builder().currentTraits(false).build(), new Context());
        } catch (final OperationException e) {
            throw new GafferRuntimeException("Unable to get Traits using GetTraits Operation", e);
        }
    }

    @GetMapping(path = "/transformFunctions", produces = APPLICATION_JSON_VALUE)
    @Operation(summary = "Gets the available transform functions")
    public Set<Class> getTransformFunctions() {
        return ReflectionUtil.getSubTypes(Function.class);
    }

    @GetMapping(path = "/aggregationFunctions", produces = APPLICATION_JSON_VALUE)
    @Operation(summary = "Gets the available aggregation functions")
    public Set<Class> getAggregationFunctions() {
        return ReflectionUtil.getSubTypes(BinaryOperator.class);
    }
}
