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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.io.FileUtils;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * {@code GraphConfig} contains configuration for Graphs. This configuration
 * is used along side a {@link uk.gov.gchq.gaffer.store.schema.Schema} and
 * {@link uk.gov.gchq.gaffer.store.StoreProperties} to create a {@link Graph}.
 * This configuration is made up of graph properties such as a graphId, {@link GraphLibrary},
 * a graph {@link View} and {@link GraphHook}s.
 * To create an instance of GraphConfig you can either use the {@link uk.gov.gchq.gaffer.graph.GraphConfig.Builder}
 * or a json file.
 *
 * @see uk.gov.gchq.gaffer.graph.GraphConfig.Builder
 */
public final class GraphConfig {
    private String graphId;
    private View view;
    private GraphLibrary library;
    private String description;
    private List<GraphHook> hooks = new ArrayList<>();

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

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public GraphLibrary getLibrary() {
        return library;
    }

    public void setLibrary(final GraphLibrary library) {
        this.library = library;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    public List<GraphHook> getHooks() {
        return hooks;
    }

    public void setHooks(final List<GraphHook> hooks) {
        if (null == hooks) {
            this.hooks.clear();
        } else {
            this.hooks = hooks;
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("graphId", graphId)
                .append("view", view)
                .append("library", library)
                .append("hooks", hooks)
                .toString();
    }

    public static class Builder {
        private GraphConfig config = new GraphConfig();

        public Builder json(final Path path) {
            try {
                return json(null != path ? Files.readAllBytes(path) : null);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to read graph config from path: " + path, e);
            }
        }

        public Builder json(final URI uri) {
            try {
                json(null != uri ? StreamUtil.openStream(uri) : null);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to read graph config from uri: " + uri, e);
            }

            return this;
        }

        public Builder json(final InputStream stream) {
            try {
                json(null != stream ? sun.misc.IOUtils.readFully(stream, stream.available(), true) : null);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to read graph config from input stream", e);
            }

            return this;
        }

        public Builder json(final byte[] bytes) {
            if (null != bytes) {
                try {
                    merge(JSONSerialiser.deserialise(bytes, GraphConfig.class));
                } catch (final IOException e) {
                    throw new IllegalArgumentException("Unable to deserialise graph config", e);
                }
            }
            return this;
        }

        public Builder merge(final GraphConfig config) {
            if (null != config) {
                if (null != config.getGraphId()) {
                    this.config.setGraphId(config.getGraphId());
                }
                if (null != config.getView()) {
                    this.config.setView(config.getView());
                }
                if (null != config.getLibrary()) {
                    this.config.setLibrary(config.getLibrary());
                }
                if (null != config.getDescription()) {
                    this.config.setDescription(config.getDescription());
                }
                this.config.getHooks().addAll(config.getHooks());
            }
            return this;
        }

        public Builder graphId(final String graphId) {
            config.setGraphId(graphId);
            return this;
        }

        public Builder library(final GraphLibrary library) {
            this.config.setLibrary(library);
            return this;
        }

        public Builder description(final String description) {
            this.config.setDescription(description);
            return this;
        }

        public Builder view(final View view) {
            this.config.setView(view);
            return this;
        }

        public Builder view(final Path view) {
            return view(null != view ? new View.Builder().json(view).build() : null);
        }

        public Builder view(final InputStream view) {
            return view(null != view ? new View.Builder().json(view).build() : null);
        }

        public Builder view(final URI view) {
            try {
                view(null != view ? StreamUtil.openStream(view) : null);
            } catch (final IOException e) {
                throw new SchemaException("Unable to read view from URI: " + view, e);
            }
            return this;
        }

        public Builder view(final byte[] jsonBytes) {
            return view(null != jsonBytes ? new View.Builder().json(jsonBytes).build() : null);
        }

        public Builder addHooks(final Path hooksPath) {
            if (null == hooksPath || !hooksPath.toFile().exists()) {
                throw new IllegalArgumentException("Unable to find graph hooks file: " + hooksPath);
            }
            final GraphHook[] hooks;
            try {
                hooks = JSONSerialiser.deserialise(FileUtils.readFileToByteArray(hooksPath.toFile()), GraphHook[].class);
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
                hook = JSONSerialiser.deserialise(FileUtils.readFileToByteArray(hookPath.toFile()), GraphHook.class);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to load graph hook file: " + hookPath, e);
            }
            return addHook(hook);
        }

        public Builder addHook(final GraphHook graphHook) {
            if (null != graphHook) {
                this.config.getHooks().add(graphHook);
            }
            return this;
        }

        public Builder addHooks(final GraphHook... graphHooks) {
            if (null != graphHooks) {
                Collections.addAll(this.config.getHooks(), graphHooks);
            }
            return this;
        }

        public GraphConfig build() {
            if (null == config.getLibrary()) {
                config.setLibrary(new NoGraphLibrary());
            }

            return config;
        }

        @Override
        public String toString() {
            return config.toString();
        }
    }
}
