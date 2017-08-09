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


import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.library.NoGraphLibrary;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

public final class GraphConfig {
    private static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();

    private String graphId;
    private View view;
    private GraphLibrary graphLibrary;
    private List<GraphHook> graphHooks;

    public GraphConfig() {
    }

    public GraphConfig(final String graphId) {
        this.graphId = graphId;
    }

    public String getGraphId() {
        return graphId;
    }

    public void setGraphId(final String graphId) {
        this.graphId = graphId;
    }

    public View getView() {
        return view;
    }

    public void setView(final View view) {
        this.view = view;
    }

    public GraphLibrary getGraphLibrary() {
        return graphLibrary;
    }

    public void setGraphLibrary(final GraphLibrary graphLibrary) {
        this.graphLibrary = graphLibrary;
    }

    public List<GraphHook> getGraphHooks() {
        return graphHooks;
    }

    public void setGraphHooks(final List<GraphHook> graphHooks) {
        this.graphHooks = graphHooks;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        final GraphConfig graphConfig = (GraphConfig) obj;

        return new EqualsBuilder()
                .append(graphId, graphConfig.graphId)
                .append(view, graphConfig.view)
                .append(graphLibrary, graphConfig.graphLibrary)
                .append(graphHooks, graphConfig.graphHooks)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(11, 41)
                .append(graphId)
                .append(view)
                .append(graphLibrary)
                .append(graphHooks)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("graphId", graphId)
                .append("view", view)
                .append("graphLibrary", graphLibrary)
                .append("graphHooks", graphHooks)
                .toString();
    }

    public static class Builder {
        private GraphConfig config = new GraphConfig();

        public Builder graphId(final String graphId) {
            config.setGraphId(graphId);
            return this;
        }

        public Builder json(final Path path) {
            try {
                return json(Files.readAllBytes(path));
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to read graph config from path: " + path, e);
            }
        }

        public Builder json(final URI uri) {
            try {
                json(StreamUtil.openStream(uri));
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to read graph config from uri: " + uri, e);
            }

            return this;
        }

        public Builder json(final InputStream stream) {
            try {
                json(sun.misc.IOUtils.readFully(stream, stream.available(), true));
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to read graph config from input stream", e);
            }

            return this;
        }

        public Builder json(final byte[] bytes) {
            try {
                return merge(JSON_SERIALISER.deserialise(bytes, GraphConfig.class));
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to deserialise graph config", e);
            }
        }

        public Builder merge(final GraphConfig config) {
            if (null != config.getGraphId()) {
                this.config.setGraphId(config.getGraphId());
            }
            if (null != config.getView()) {
                this.config.setView(config.getView());
            }
            if (null != config.getGraphLibrary()) {
                this.config.setGraphLibrary(config.getGraphLibrary());
            }
            this.config.getGraphHooks().addAll(config.getGraphHooks());
            return this;
        }

        public Builder library(final GraphLibrary library) {
            this.config.setGraphLibrary(library);
            return this;
        }

        public Builder view(final View view) {
            this.config.setView(view);
            return this;
        }

        public Builder view(final Path view) {
            return view(new View.Builder().json(view).build());
        }

        public Builder view(final InputStream view) {
            return view(new View.Builder().json(view).build());
        }

        public Builder view(final URI view) {
            try {
                view(StreamUtil.openStream(view));
            } catch (final IOException e) {
                throw new SchemaException("Unable to read view from URI: " + view, e);
            }
            return this;
        }

        public Builder view(final byte[] jsonBytes) {
            return view(new View.Builder().json(jsonBytes).build());
        }

        public Builder addHooks(final Path hooksPath) {
            if (null == hooksPath || !hooksPath.toFile().exists()) {
                throw new IllegalArgumentException("Unable to find graph hooks file: " + hooksPath);
            }
            final GraphHook[] hooks;
            try {
                hooks = JSON_SERIALISER.deserialise(FileUtils.readFileToByteArray(hooksPath.toFile()), GraphHook[].class);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to load graph hooks file: " + hooksPath, e);
            }
            return addHooks(hooks);
        }

        public Builder addHook(final Path hookPath) {
            if (null == hookPath || !hookPath.toFile().exists()) {
                throw new IllegalArgumentException("Unable to find graph hook file: " + hookPath);
            }

            final GraphHook hook;
            try {
                hook = JSON_SERIALISER.deserialise(FileUtils.readFileToByteArray(hookPath.toFile()), GraphHook.class);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to load graph hook file: " + hookPath, e);
            }
            return addHook(hook);
        }

        public Builder addHook(final GraphHook graphHook) {
            this.config.getGraphHooks().add(graphHook);
            return this;
        }

        public Builder addHooks(final GraphHook... graphHooks) {
            Collections.addAll(this.config.getGraphHooks(), graphHooks);
            return this;
        }

        public GraphConfig build() {
            if (null == config.getGraphLibrary()) {
                config.setGraphLibrary(new NoGraphLibrary());
            }

            return config;
        }
    }
}
