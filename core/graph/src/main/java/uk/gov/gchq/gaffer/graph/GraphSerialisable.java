/*
 * Copyright 2017-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.graph;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * A Serialisable object which holds the contents for creating Graphs.
 * Does not store all the graph data, this only is used to recreate the graph
 * after serialisation from the graph elements.
 *
 * @see GraphSerialisable.Builder
 */
@JsonDeserialize(builder = GraphSerialisable.Builder.class)
public class GraphSerialisable implements Serializable {
    private static final long serialVersionUID = 2684203367656032583L;
    private final byte[] serialisedSchema;
    private final byte[] serialisedProperties;
    private final byte[] serialisedConfig;
    private transient Schema schema;
    private transient StoreProperties storeProperties;
    private transient GraphConfig config;

    private transient Graph graph;

    public GraphSerialisable(final GraphConfig config, final Schema schema, final StoreProperties storeProperties) {
        this(config, schema, storeProperties.getProperties());
    }

    public GraphSerialisable(final GraphConfig config, final Schema schema, final Properties properties) {
        try {
            this.serialisedSchema = isNull(schema) ? null : JSONSerialiser.serialise(schema, true);
        } catch (final SerialisationException e) {
            throw new IllegalArgumentException("Unable to serialise schema", e);
        }

        try {
            this.serialisedConfig = isNull(config) ? null : JSONSerialiser.serialise(config, true);
        } catch (final SerialisationException e) {
            throw new IllegalArgumentException("Unable to serialise config", e);
        }

        try {
            this.serialisedProperties = isNull(properties) ? null : JSONSerialiser.serialise(properties, true);
        } catch (final SerialisationException e) {
            throw new IllegalArgumentException("Unable to serialise properties", e);
        }
    }


    /**
     * @return returns a new {@link Graph} built from the contents of a this
     * class.
     */
    @JsonIgnore
    public Graph getGraph() {
        return getGraph(null);
    }

    /**
     * @param library the library to use and add into the builder.
     * @return returns a new {@link Graph} built from the contents of a this
     * class.
     */
    @JsonIgnore
    public Graph getGraph(final GraphLibrary library) {
        if (isNull(graph)) {

            graph = new Graph.Builder()
                    .addSchema(getSchema())
                    .addStoreProperties(getStoreProperties())
                    .config(getConfig())
                    .config(new GraphConfig.Builder()
                            .library(library)
                            .build())
                    .addToLibrary(false)
                    .build();
        }
        return graph;
    }

    @Override
    public boolean equals(final Object obj) {
        final Boolean rtn;
        if (isNull(obj) || !(obj instanceof GraphSerialisable)) {
            rtn = false;
        } else {
            final GraphSerialisable that = (GraphSerialisable) obj;
            rtn = new EqualsBuilder()
                    .appendSuper(Arrays.equals(this.getSerialisedConfig(), that.getSerialisedConfig()))
                    .appendSuper(Arrays.equals(this.getSerialisedSchema(), that.getSerialisedSchema()))
                    .appendSuper(Arrays.equals(this.getSerialisedProperties(), that.getSerialisedProperties()))
                    .build();
        }
        return rtn;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("config", getConfig())
                .append("schema", getSchema())
                .append("properties", getStoreProperties().getProperties())
                .build();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(13, 31)
                .append(this.getSerialisedConfig())
                .append(this.getSerialisedSchema())
                .append(this.getSerialisedProperties())
                .build();
    }

    @JsonIgnore
    private Schema _getDeserialisedSchema() {
        if (isNull(schema)) {
            if (isNull(graph)) {
                schema = null != serialisedSchema ? Schema.fromJson(serialisedSchema) : null;
            } else {
                schema = graph.getSchema();
            }
        }
        return schema;
    }

    @JsonIgnore
    public Schema getSchema() {
        return getSchema(null);
    }

    @JsonIgnore
    public Schema getSchema(final GraphLibrary graphLibrary) {
        Schema schema = _getDeserialisedSchema();
        if (isNull(schema) && nonNull(graphLibrary)) {
            schema = graphLibrary.getSchema(graph.getGraphId());
        }
        return schema;
    }

    @JsonIgnore
    public String getGraphId() {
        GraphConfig graphConfig = getConfig();
        return nonNull(graphConfig)
                ? graphConfig.getGraphId()
                : null;
    }

    public byte[] getSerialisedSchema() {
        return serialisedSchema;
    }

    @JsonIgnore
    private StoreProperties _getDeserialisedStoreProperties() {
        if (isNull(storeProperties)) {
            if (isNull(graph)) {
                try {
                    storeProperties = null != serialisedProperties ? StoreProperties.loadStoreProperties(JSONSerialiser.deserialise(serialisedProperties, Properties.class)) : null;
                } catch (final SerialisationException e) {
                    throw new GafferRuntimeException("Unable to deserialise properties", e);
                }
            } else {
                storeProperties = graph.getStoreProperties();
            }
        }
        return storeProperties;
    }

    @JsonIgnore
    public StoreProperties getStoreProperties() {
        return getStoreProperties(null);
    }

    @JsonIgnore
    public StoreProperties getStoreProperties(final GraphLibrary graphLibrary) {
        StoreProperties properties = _getDeserialisedStoreProperties();
        if (isNull(properties) && nonNull(graphLibrary)) {
            properties = graphLibrary.getProperties(graph.getGraphId());
        }
        return properties;
    }

    public byte[] getSerialisedProperties() {
        return serialisedProperties;
    }

    @JsonIgnore
    public GraphConfig getConfig() {
        if (isNull(config)) {
            if (isNull(graph)) {
                config = null != serialisedConfig ? new GraphConfig.Builder().json(serialisedConfig).build() : null;
            } else {
                config = graph.getConfig();
            }
        }
        return config;
    }

    public byte[] getSerialisedConfig() {
        return serialisedConfig;
    }

    @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
    public static class Builder {

        private Schema schema;
        private Properties properties;
        private GraphConfig config;

        public Builder() {
        }

        @JsonIgnore
        public Builder(final Graph graph) {
            this();
            schema(graph.getSchema());
            properties(graph.getStoreProperties().getProperties());
            config(graph.getConfig());
        }

        public Builder(final GraphSerialisable graphSerialisable) {
            this();
            schema(graphSerialisable.getSchema());
            properties(graphSerialisable.getStoreProperties());
            config(graphSerialisable.getConfig());
        }

        public Builder schema(final Schema schema) {
            this.schema = schema;
            return _self();
        }

        public Builder schema(final InputStream schema) {
            return schema(Schema.fromJson(schema));
        }

        @JsonSetter("properties")
        public Builder properties(final Properties properties) {
            this.properties = properties;
            return _self();
        }

        public Builder properties(final StoreProperties properties) {
            if (isNull(properties)) {
                this.properties = null;
            } else {
                this.properties = properties.getProperties();
            }
            return _self();
        }

        public Builder properties(final InputStream properties) {
            return properties(StoreProperties.loadStoreProperties(properties));
        }

        public Builder config(final GraphConfig config) {
            this.config = config;
            return _self();
        }

        public Builder mergeConfig(final GraphConfig config) {
            this.config = new GraphConfig.Builder()
                    .merge(this.config)
                    .merge(config)
                    .build();

            return _self();
        }

        private Builder _self() {
            return this;
        }

        public GraphSerialisable build() {
            if (isNull(config) || isNull(config.getGraphId())) {
                throw new IllegalArgumentException("GraphSerialisable Builder requires a graph name");
            }
            return new GraphSerialisable(config, schema, properties);
        }
    }
}
