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
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.core.exception.Status;
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

import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;

@RestController
public class GraphConfigurationController implements IGraphConfigurationController {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphConfigurationController.class);

    private GraphFactory graphFactory;

    @Autowired
    public void setGraphFactory(final GraphFactory graphFactory) {
        this.graphFactory = graphFactory;
    }

    @Override
    public ResponseEntity<Schema> getSchema() {
        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .body(graphFactory.getGraph().getSchema());
    }

    @Override
    public ResponseEntity<String> getDescription() {
        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .body(graphFactory.getGraph().getDescription());
    }

    @Override
    public ResponseEntity<Set<Class>> getFilterFunctions() {
        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .body(ReflectionUtil.getSubTypes(Predicate.class));
    }

    @Override
    public ResponseEntity<Set<Class>> getElementGenerators() {
        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .body(ReflectionUtil.getSubTypes(ElementGenerator.class));
    }

    @Override
    public ResponseEntity<Set<Class>> getFilterFunctions(@PathVariable("inputClass") final String inputClass) {
        if (StringUtils.isEmpty(inputClass)) {
            return getFilterFunctions();
        }

        final Class<?> clazz;
        try {
            clazz = Class.forName(SimpleClassNameIdResolver.getClassName(inputClass));
        } catch (final ClassNotFoundException e) {
            throw new GafferRuntimeException("Could not find class on the classpath:" + inputClass, e, Status.NOT_FOUND);
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
            if (inputSignature.assignable(clazz).isValid()) {
                applicablePredicates.add(predicateClass);
            }
        }

        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .body(applicablePredicates);
    }

    @Override
    public ResponseEntity<Set<Class>> getObjectGenerators() {
        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .body(ReflectionUtil.getSubTypes(ObjectGenerator.class));
    }


    @Override
    public ResponseEntity<Set<String>> getSerialisedFields(@PathVariable("className") final String className) {
        Map<String, String> serialisedFieldClasses = JsonSerialisationUtil.getSerialisedFieldClasses(className);
        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .body(serialisedFieldClasses.keySet());
    }

    @Override
    public ResponseEntity<Map<String, String>> getSerialisedFieldClasses(@PathVariable("className") final String className) {
        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .body(JsonSerialisationUtil.getSerialisedFieldClasses(className));
    }

    @Override
    public ResponseEntity<Set<StoreTrait>> getStoreTraits() {
        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .body(graphFactory.getGraph().getStoreTraits());
    }

    @Override
    public ResponseEntity<Set<Class>> getTransformFunctions() {
        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .body(ReflectionUtil.getSubTypes(Function.class));
    }
}
