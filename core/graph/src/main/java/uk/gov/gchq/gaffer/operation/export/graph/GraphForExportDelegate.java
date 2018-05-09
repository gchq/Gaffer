/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.export.graph;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.List;
import java.util.Set;

public class GraphForExportDelegate extends GraphDelegate {

    @Override
    public StoreProperties resolveStorePropertiesForGraph(final Store store, final StoreProperties properties, final String parentStorePropertiesId, final Pair<Schema, StoreProperties> existingGraphPair) {
        StoreProperties resultProps = super.resolveStorePropertiesForGraph(store, properties, parentStorePropertiesId, existingGraphPair);
        if (null == resultProps) {
            // If no properties have been provided then default to using the store properties
            resultProps = store.getProperties();
        }
        return resultProps;
    }

    @Override
    public Schema resolveSchemaForGraph(final Store store, final Schema schema, final List<String> parentSchemaIds, final Pair<Schema, StoreProperties> existingGraphPair) {
        Schema resultSchema = super.resolveSchemaForGraph(store, schema, parentSchemaIds, existingGraphPair);
        if (null == resultSchema) {
            // If no schemas have been provided then default to using the store schema
            resultSchema = store.getSchema();
        }
        return resultSchema;
    }

    @Override
    public void validateGraph(final Store store, final String graphId, final Schema schema, final StoreProperties storeProperties, final List<String> parentSchemaIds, final String parentStorePropertiesId, final Pair<Schema, StoreProperties> existingGraphPair) {
        ValidationResult result = super.validate(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId, existingGraphPair, new ValidationResult());

        Set<String> errors = result.getErrors();

        result.getErrors().removeIf(s -> s.equals(String.format(CANT_BOTH_BE_NULL, SCHEMA_STRING, PARENT_SCHEMA_IDS))
                || s.equals(String.format(CANT_BOTH_BE_NULL, STORE_PROPERTIES_STRING, PARENT_STORE_PROPERTIES_ID)));

        result = new ValidationResult();
        for (final String error : errors) {
            result.addError(error);
        }

        if (graphId.equals(store.getGraphId())) {
            result.addError(String.format(CANNOT_EXPORT_TO_THE_SAME_GRAPH_S, graphId));
        }
        if (!result.isValid()) {
            throw new IllegalArgumentException(result.getErrorString());
        }
    }

    public static class Builder extends BaseBuilder<Builder> {
        @Override
        public Builder _self() {
            return this;
        }

        @Override
        public Graph createGraph() {
            return new GraphForExportDelegate().createGraphInstance(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId);
        }
    }
}
