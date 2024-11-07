/*
 * Copyright 2017-2024 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.hook.GetFromCacheHook;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.gaffer.graph.hook.GraphHookPath;
import uk.gov.gchq.gaffer.graph.hook.exception.GraphHookException;
import uk.gov.gchq.gaffer.graph.hook.exception.GraphHookSuffixException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.library.NoGraphLibrary;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddToCacheHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.File;
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
 * If you wish to write a GraphHook in a separate json file and include it, you
 * can do it by using the {@link GraphHookPath} GraphHook and setting the path field within.
 *
 * @see uk.gov.gchq.gaffer.graph.GraphConfig.Builder
 */
@JsonPropertyOrder(value = {"description", "graphId", "otelActive"}, alphabetic = true)
public final class GraphConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphConfig.class);

    private String graphId;
    // Keeping the view as json enforces a new instance of View is created
    // every time it is used.
    private byte[] view;
    private GraphLibrary library;
    private String description;
    private final List<GraphHook> hooks = new ArrayList<>();
    private Boolean otelActive;

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
        return null != view ? View.fromJson(view) : null;
    }

    public void setView(final View view) {
        this.view = null != view ? view.toCompactJson() : null;
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

    /**
     * Returns a list of all the graph hooks.
     *
     * @return List of graph hooks.
     */
    public List<GraphHook> getHooks() {
        return hooks;
    }

    public void setHooks(final List<GraphHook> hooks) {
        if (null == hooks) {
            this.hooks.clear();
        } else {
            hooks.forEach(this::addHook);
        }
    }

    /**
     * Is OpenTelemtery logging set to be active
     * @return True if active
     */
    public Boolean getOtelActive() {
        return otelActive;
    }

    /**
     * Set OpenTelemetry logging to be active
     * @param otelActive is active
     */
    public void setOtelActive(final Boolean otelActive) {
        this.otelActive = otelActive;
    }

    /**
     * Adds the supplied {@link GraphHook} to the list of graph hooks.
     *
     * @param hook The hook to add.
     */
    public void addHook(final GraphHook hook) {
        // Validate params
        if (hook == null) {
            LOGGER.warn("addHook was called with a null hook, ignoring call");
            return;
        }

        if (hook instanceof GraphHookPath) {
            final File file = new File(((GraphHookPath) hook).getPath());
            if (!file.exists()) {
                throw new IllegalArgumentException("Unable to find graph hook file: " + file.toString());
            }
            try {
                LOGGER.debug("Adding Hook from JSON file path: {}", file);
                hooks.add(JSONSerialiser.deserialise(FileUtils.readFileToByteArray(file), GraphHook.class));
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to deserialise graph hook from file: " + file.toString(), e);
            }
        } else {
            LOGGER.debug("Adding Hook {}", hook.getClass().getSimpleName());
            hooks.add(hook);
        }
    }

    /**
     * Checks the current {@link GraphHook} list to see if any of the hooks
     * match the supplied class. Will return true if list contains one or more
     * matches.
     *
     * @param hookClass Class to check for match.
     * @return True if hook with matching class found.
     */
    public boolean hasHook(final Class<? extends GraphHook> hookClass) {
        return hooks.stream().anyMatch(hook -> hookClass.isAssignableFrom(hook.getClass()));
    }

    /**
     * Extracts and compares the cache suffixes of the supplied {@link Operation}'s handler
     * and {@link GetFromCacheHook} resolver hook, throws {@link GraphHookSuffixException}
     * if mismatched as writing and reading to cache will not behave correctly.
     * <p>
     * Will attempt to add the supplied {@link GetFromCacheHook} to the graph config
     * if not currently present.
     *
     * @param store The Store the operationClass is for
     * @param operationClass The Operation requiring cache write
     * @param hookClass The Hook requiring cache reading
     * @param suffixFromProperties The suffix from property
     */
    public void validateAndUpdateGetFromCacheHook(final Store store, final Class<? extends Operation> operationClass, final Class<? extends GetFromCacheHook> hookClass, final String suffixFromProperties) {
        if (!store.isSupported(operationClass)) {
            LOGGER.warn(
                "The current store type: {} does not support the operation: {} unable to validate cache hook",
                store.getClass().getSimpleName(),
                operationClass.getSimpleName());
            return;
        }

        // Use the suffix from the properties as a fall back if the operation handler is not correct type
        String suffix = suffixFromProperties;

        // Get Handler for the operation to try extract the cache suffix if applicable class
        final OperationHandler<Operation> addToCacheHandler = store.getOperationHandler(operationClass);
        if (AddToCacheHandler.class.isAssignableFrom(addToCacheHandler.getClass())) {
            // Extract the Suffix
            suffix = ((AddToCacheHandler<?>) addToCacheHandler).getSuffixCacheName();
        } else {
            // Otherwise log warning and continue with the suffix from the properties
            LOGGER.warn(
                "Handler for: {} was not expected type: {}. Can't get suffixCache using value from property: {}",
                operationClass,
                AddToCacheHandler.class.getSimpleName(),
                suffixFromProperties);
        }

        // Is the supplied GetFromCacheHook class missing from the config
        if (!hasHook(hookClass)) {
            try {
                // Provide info about the graph not having the required hook
                LOGGER.info(
                    "For GraphID: {} a handler was supplied for Operation: {}, but without a {}, adding {} with suffix: {}",
                    getGraphId(),
                    operationClass,
                    hookClass.getSimpleName(),
                    hookClass.getSimpleName(),
                    suffix);
                // Try add the hook
                hooks.add(0, hookClass.getDeclaredConstructor(String.class).newInstance(suffix));
            } catch (final Exception e) {
                throw new GraphHookException(e.getMessage(), e);
            }
        } else {
            // Find the relevant hook
            final GetFromCacheHook nvrHook = (GetFromCacheHook) getHooks().stream()
                .filter(hook -> hookClass.isAssignableFrom(hook.getClass()))
                .findAny()
                .orElseThrow(() -> new GraphHookException(
                        String.format("Unable to find matching hook in graph config for class %s", hookClass.getSimpleName())));

            // Validate the suffix for a mismatch
            final String nvrSuffix = nvrHook.getSuffixCacheName();
            if (!suffix.equals(nvrSuffix)) {
                //Error
                throw new GraphHookSuffixException(
                    String.format(
                        "%s hook is configured with suffix: %s and %s handler is configured with suffix: %s this causes a cache reading and writing misalignment.",
                        hookClass.getSimpleName(),
                        nvrSuffix,
                        addToCacheHandler.getClass().getSimpleName(),
                        suffix));
            }
        }

    }

    /**
     * Initialises the {@link View} for the graph config based on supplied schema.
     *
     * @param schema The schema to set the View from.
     */
    public void initView(final Schema schema) {
        if (getView() != null) {
            LOGGER.debug("View already set ignoring initView call");
            return;
        }

        setView(new View.Builder()
            .entities(schema.getEntityGroups())
            .edges(schema.getEdgeGroups())
            .build());
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("graphId", graphId)
                .append("view", getView())
                .append("library", library)
                .append("hooks", hooks)
                .append("otelActive", otelActive)
                .toString();
    }

    public static class Builder {
        private final GraphConfig config = new GraphConfig();

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
                json(null != stream ? IOUtils.toByteArray(stream) : null);
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
            if (config == null) {
                LOGGER.warn("Unable to merge GraphConfig with a null config, ignoring call");
                return this;
            }

            if (config.getGraphId() != null) {
                this.config.setGraphId(config.getGraphId());
            }
            if (config.getView() != null) {
                this.config.setView(config.getView());
            }
            if (config.getLibrary() != null && !(config.getLibrary() instanceof NoGraphLibrary)) {
                this.config.setLibrary(config.getLibrary());
            }
            if (config.getDescription() != null) {
                this.config.setDescription(config.getDescription());
            }
            if (config.getOtelActive() != null) {
                this.config.setOtelActive(config.getOtelActive());
            }
            this.config.getHooks().addAll(config.getHooks());

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

        public Builder otelActive(final Boolean active) {
            this.config.setOtelActive(active);
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
                this.config.addHook(graphHook);
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
