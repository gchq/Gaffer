/*
 * Copyright 2017 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.Serializable;
import java.util.Properties;

/**
 * A Serialisable object which holds the contents for creating Graphs.
 * Does not store all the graph data, this only is used to recreate the graph
 * after serialisation from the graph elements.
 *
 * @see GraphSerialisable.Builder
 */
@JsonDeserialize(builder = GraphSerialisable.Builder.class)
public final class GraphSerialisable implements Serializable {
    private static final long serialVersionUID = 2684203367656032583L;
    private final byte[] schema;
    private final Properties properties;
    private final byte[] config;
    private transient Graph graph;

    private GraphSerialisable(final GraphConfig config, final Schema schema, final Properties properties) {
        try {
            this.schema = null == schema ? null : JSONSerialiser.serialise(schema, true);
        } catch (final SerialisationException e) {
            throw new IllegalArgumentException("Unable to serialise schema", e);
        }
        try {
            this.config = null == config ? null : JSONSerialiser.serialise(config, true);
        } catch (final SerialisationException e) {
            throw new IllegalArgumentException("Unable to serialise config", e);
        }
        this.properties = properties;
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
        if (null == graph) {
            graph = new Graph.Builder()
                    .addSchema(schema)
                    .addStoreProperties(properties)
                    .config(config)
                    .config(new GraphConfig.Builder()
                            .library(library)
                            .build())
                    .build();
        }

        return graph;
    }

    @Override
    public boolean equals(final Object obj) {
        final Boolean rtn;
        if (null == obj || !(obj instanceof GraphSerialisable)) {
            rtn = false;
        } else {
            final GraphSerialisable that = (GraphSerialisable) obj;
            rtn = new EqualsBuilder()
                    .append(this.getConfig(), that.getConfig())
                    .append(this.getSchema(), that.getSchema())
                    .append(this.getProperties(), that.getProperties())
                    .build();
        }
        return rtn;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("config", StringUtil.toString(this.getConfig()))
                .append("schema", StringUtil.toString(this.getSchema()))
                .append("properties", this.getProperties())
                .build();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(13, 31)
                .append(this.getConfig())
                .append(this.getSchema())
                .append(this.getProperties())
                .build();
    }

    public byte[] getSchema() {
        return schema;
    }

    public Properties getProperties() {
        return properties;
    }

    public byte[] getConfig() {
        return config;
    }

    @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
    public static class Builder {

        private Schema schema;
        private Properties properties;
        private GraphConfig config;

        public Builder schema(final Schema schema) {
            this.schema = schema;
            return _self();
        }

        @JsonSetter("properties")
        public Builder properties(final Properties properties) {
            this.properties = properties;
            return _self();
        }

        public Builder properties(final StoreProperties properties) {
            if (null == properties) {
                this.properties = null;
            } else {
                this.properties = properties.getProperties();
            }
            return _self();
        }

        public Builder config(final GraphConfig config) {
            this.config = config;
            return _self();
        }

        @JsonIgnore
        public Builder graph(final Graph graph) {
            schema = graph.getSchema();
            properties = graph.getStoreProperties().getProperties();
            config = graph.getConfig();
            return _self();
        }

        private Builder _self() {
            return this;
        }

        public GraphSerialisable build() {
            return new GraphSerialisable(config, schema, properties);
        }
    }

}
