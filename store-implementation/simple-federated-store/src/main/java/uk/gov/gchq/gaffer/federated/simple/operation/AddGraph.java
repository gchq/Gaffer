/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple.operation;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.federated.simple.access.GraphAccess;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;
import java.util.Properties;

@Since("2.4.0")
@Summary("Adds a new Graph to the federated store")
@JsonPropertyOrder(value = { "class", "graphConfig" }, alphabetic = true)
public class AddGraph implements Operation {

    @Required
    private GraphConfig graphConfig;
    private Schema schema;
    private Properties properties;
    private GraphAccess graphAccess;
    private Map<String, String> options;

    // Getters

    /**
     * Get current set {@link GraphConfig}.
     *
     * @return The graph config.
     */
    public GraphConfig getGraphConfig() {
        return graphConfig;
    }

    /**
     * Get the current set {@link Schema}.
     *
     * @return The schema.
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Get the current set {@link Properties} for the store.
     *
     * @return The properties for the store.
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Get the current set {@link GraphAccess}.
     *
     * @return The Graph access.
     */
    public GraphAccess getGraphAccess() {
        return graphAccess;
    }

    // Setters

    /**
     * Set the {@link GraphConfig}.
     *
     * @param graphConfig The config to set.
     */
    public void setGraphConfig(final GraphConfig graphConfig) {
        this.graphConfig = graphConfig;
    }

    /**
     * Set the {@link Schema}.
     *
     * @param schema The schema to set.
     */
    public void setSchema(final Schema schema) {
        this.schema = schema;
    }

    /**
     * Set the {@link Properties} for the store.
     *
     * @param properties The properties to set.
     */
    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    /**
     * Set the {@link GraphAccess} for the graph.
     *
     * @param graphAccess The graph access to set.
     */
    public void setGraphAccess(final GraphAccess graphAccess) {
        this.graphAccess = graphAccess;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public Operation shallowClone() throws CloneFailedException {
        return new AddGraph.Builder()
                .graphConfig(graphConfig)
                .schema(schema)
                .properties(properties)
                .graphAccess(graphAccess)
                .options(options)
                .build();
    }

    public static class Builder extends Operation.BaseBuilder<AddGraph, Builder> {
        public Builder() {
            super(new AddGraph());
        }

        /**
         * Set the {@link GraphConfig}.
         *
         * @param graphConfig The config to set.
         * @return The builder.
         */
        public Builder graphConfig(final GraphConfig graphConfig) {
            _getOp().setGraphConfig(graphConfig);
            return _self();
        }

        /**
         * Set the {@link Schema}.
         *
         * @param schema The schema to set.
         * @return The builder.
         */
        public Builder schema(final Schema schema) {
            _getOp().setSchema(schema);
            return _self();
        }

        /**
         * Set the {@link Properties} for the store.
         *
         * @param properties The properties to set.
         * @return The builder.
         */
        public Builder properties(final Properties properties) {
            _getOp().setProperties(properties);
            return _self();
        }

        /**
         * Set the {@link GraphAccess}.
         *
         * @param graphAccess The graph access to set.
         * @return The builder.
         */
        public Builder graphAccess(final GraphAccess graphAccess) {
            _getOp().setGraphAccess(graphAccess);
            return _self();
        }
    }
}
