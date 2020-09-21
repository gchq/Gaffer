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

import org.springframework.http.RequestEntity;

import uk.gov.gchq.gaffer.data.generator.ElementGenerator;
import uk.gov.gchq.gaffer.data.generator.ObjectGenerator;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

public class GraphConfigurationController implements IGraphConfigurationController {
    // todo this
    @Override
    public RequestEntity<Schema> getSchema() {
        return null;
    }

    @Override
    public RequestEntity<String> getDescription() {
        return null;
    }

    @Override
    public RequestEntity<Set<? extends Predicate>> getFilterFunctions() {
        return null;
    }

    @Override
    public RequestEntity<Set<? extends ElementGenerator>> getElementGenerators() {
        return null;
    }

    @Override
    public RequestEntity<Set<? extends Predicate>> getFilterFunctions(final String inputClass) {
        return null;
    }

    @Override
    public RequestEntity<Set<? extends ObjectGenerator>> getObjectGenerators() {
        return null;
    }

    @Override
    public RequestEntity<Set<String>> getSerialisedFields(final String className) {
        return null;
    }

    @Override
    public RequestEntity<Map<String, String>> getSerialisedFieldClasses(final String className) {
        return null;
    }

    @Override
    public RequestEntity<Set<StoreTrait>> getStoreTraits() {
        return null;
    }

    @Override
    public RequestEntity<Set<? extends Function>> getTransformFunctions() {
        return null;
    }
}
