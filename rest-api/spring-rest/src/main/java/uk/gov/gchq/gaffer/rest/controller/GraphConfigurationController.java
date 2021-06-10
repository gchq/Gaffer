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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import uk.gov.gchq.gaffer.data.generator.ElementGenerator;
import uk.gov.gchq.gaffer.data.generator.ObjectGenerator;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.serialisation.util.JsonSerialisationUtil;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;
import uk.gov.gchq.koryphe.signature.Signature;
import uk.gov.gchq.koryphe.util.ReflectionUtil;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

@RestController
public class GraphConfigurationController implements IGraphConfigurationController {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphConfigurationController.class);

    private GraphFactory graphFactory;

    @Autowired
    public GraphConfigurationController(final GraphFactory graphFactory) {
        this.graphFactory = graphFactory;
    }

    @Override
    public Schema getSchema() {
        return graphFactory.getGraph().getSchema();
    }

    @Override
    public String getDescription() {
        return graphFactory.getGraph().getDescription();
    }

    @Override
    public String getGraphId() {
        return graphFactory.getGraph().getGraphId();
    }

    @Override
    public Set<Class> getFilterFunctions() {
        return ReflectionUtil.getSubTypes(Predicate.class);
    }

    @Override
    public Set<Class> getElementGenerators() {
        return ReflectionUtil.getSubTypes(ElementGenerator.class);
    }

    @Override
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
                LOGGER.warn("Failed to create new instance of " + predicateClass, e);
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

    @Override
    public Set<Class> getObjectGenerators() {
        return ReflectionUtil.getSubTypes(ObjectGenerator.class);
    }


    @Override
    public Set<String> getSerialisedFields(@PathVariable("className") final String className) {
        return JsonSerialisationUtil.getSerialisedFieldClasses(className).keySet();
    }


    @Override
    public Map<String, String> getSerialisedFieldClasses(@PathVariable("className") final String className) {
        return JsonSerialisationUtil.getSerialisedFieldClasses(className);
    }

    @Override
    public Set<StoreTrait> getStoreTraits() {
        return graphFactory.getGraph().getStoreTraits();
    }

    @Override
    public Set<Class> getTransformFunctions() {
        return ReflectionUtil.getSubTypes(Function.class);
    }
}
