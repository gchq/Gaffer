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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
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
public final class GraphSerialisable implements Serializable {
    private static final long serialVersionUID = 2684203367656032583L;
    private byte[] schema;
    private Properties properties;
    private byte[] config;

    public GraphSerialisable() {
    }

    private GraphSerialisable(final GraphConfig config, final Schema schema, final Properties properties) {
        try {
            this.schema = JSONSerialiser.serialise(schema, true);
            this.config = JSONSerialiser.serialise(config, true);
        } catch (final SerialisationException e) {
            throw new IllegalArgumentException("Unable to serialise given parameter", e);
        }
        this.properties = properties;
    }

    /**
     * @return returns a new {@link Graph} built from the contents of a this
     * class.
     */
    public Graph buildGraph() {
        return buildGraph(null);
    }

    /**
     * @return returns a new {@link Graph} built from the contents of a this
     * class.
     */
    public Graph buildGraph(final GraphLibrary library) {
        return new Graph.Builder()
                .addSchema(schema)
                .addStoreProperties(properties)
                .config(config)
                .config(new GraphConfig.Builder().library(library).build())
                .build();
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

    public static class Builder {

        private Schema schema;
        private Properties properties;
        private GraphConfig config;

        public Builder setSchema(final Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder setProperties(final Properties properties) {
            this.properties = properties;
            return this;
        }

        public Builder setConfig(final GraphConfig config) {
            this.config = config;
            return this;
        }

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
