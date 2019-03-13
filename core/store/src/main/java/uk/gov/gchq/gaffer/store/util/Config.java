/*
 * Copyright 2019 Crown Copyright
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
package uk.gov.gchq.gaffer.store.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.koryphe.util.ReflectionUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Config {
    /**
     * The id of the store.
     */
    private String id;

    /**
     * A short description of the store
     */
    private String description;

    /**
     * A list of {@link Hook}s
     */
    private List<Hook> hooks = new ArrayList<>();

    /**
     * The store properties - contains specific configuration information for
     * the store - such as database connection strings.
     */
    private StoreProperties properties;

    /**
     * The operation handlers - A Map containing all classes of operations
     * supported by this store, and an instance of all the OperationHandlers
     * that will be used to handle these operations.
     */
    private final Map<Class<? extends Operation>, OperationHandler> operationHandlers = new LinkedHashMap<>();

    public Config() {
    }

    public Config(final String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public List<Hook> getHooks() {
        return hooks;
    }

    public void setHooks(final List<Hook> hooks) {
        if (null == hooks) {
            this.hooks.clear();
        } else {
            hooks.forEach(this::addHook);
        }
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    public void addHook(final Hook hook) {
        if (null != hook) {
            if (hook instanceof HookPath) {
                final String path = ((HookPath) hook).getPath();
                final File file = new File(path);
                if (!file.exists()) {
                    throw new IllegalArgumentException("Unable to find graph hook file: " + path);
                }
                try {
                    hooks.add(JSONSerialiser.deserialise(FileUtils.readFileToByteArray(file), Hook.class));
                } catch (final IOException e) {
                    throw new IllegalArgumentException("Unable to deserialise graph hook from file: " + path, e);
                }
            } else {
                hooks.add(hook);
            }
        }
    }

    /**
     * Get this Store's {@link uk.gov.gchq.gaffer.store.StoreProperties}.
     *
     * @return the instance of {@link uk.gov.gchq.gaffer.store.StoreProperties},
     * this may contain details such as database connection details.
     */
    public StoreProperties getProperties() {
        return properties;
    }

    public void setProperties(final StoreProperties properties) {
        final Class<? extends StoreProperties> requiredPropsClass = getPropertiesClass();
        properties.updateStorePropertiesClass(requiredPropsClass);

        // If the properties instance is not already an instance of the required class then reload the properties
        if (requiredPropsClass.isAssignableFrom(properties.getClass())) {
            this.properties = properties;
        } else {
            this.properties = StoreProperties.loadStoreProperties(properties.getProperties());
        }

        ReflectionUtil.addReflectionPackages(properties.getReflectionPackages());
        updateJsonSerialiser();
    }

    protected Class<? extends StoreProperties> getPropertiesClass() {
        return StoreProperties.class;
    }

    public static void updateJsonSerialiser(final StoreProperties storeProperties) {
        if (null != storeProperties) {
            JSONSerialiser.update(
                    storeProperties.getJsonSerialiserClass(),
                    storeProperties.getJsonSerialiserModules(),
                    storeProperties.getStrictJson()
            );
        } else {
            JSONSerialiser.update();
        }
    }

    public void updateJsonSerialiser() {
        updateJsonSerialiser(properties);
    }

    public void addOperationHandler(final Class<? extends Operation> opClass, final OperationHandler handler) {
        if (null == handler) {
            operationHandlers.remove(opClass);
        } else {
            operationHandlers.put(opClass, handler);
        }
    }

    public <OP extends Output<O>, O> void addOperationHandler(final Class<? extends Output<O>> opClass, final OutputOperationHandler<OP, O> handler) {
        if (null == handler) {
            operationHandlers.remove(opClass);
        } else {
            operationHandlers.put(opClass, handler);
        }
    }

    public OperationHandler<Operation> getOperationHandler(final Class<? extends Operation> opClass) {
        return operationHandlers.get(opClass);
    }

    public Map<Class<? extends Operation>, OperationHandler> getOperationHandlers() {
        return operationHandlers;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("description", description)
                .append("hooks", hooks)
                .append("properties", properties)
                .append("operationHandlers", operationHandlers)
                .toString();
    }

    interface Builder<conf, B extends Builder<conf, ?>> {
        conf _getConf();

        B _self();
    }

    public static class BaseBuilder<conf extends Config,
            B extends BaseBuilder> implements Builder {
        private conf config;
        private List<Hook> hooks;
        private StoreProperties properties;

        // Id
        public B id(final String id) {
            config.setId(id);
            return _self();
        }

        // Description
        public B description(final String description) {
            config.setDescription(description);
            return _self();
        }

        // StoreProperties
        public B storeProperties(final Properties properties) {
            return storeProperties(null != properties ? StoreProperties.loadStoreProperties(properties) : null);
        }

        public B storeProperties(final StoreProperties properties) {
            this.properties = properties;
            if (null != properties) {
                ReflectionUtil.addReflectionPackages(properties.getReflectionPackages());
                JSONSerialiser.update(
                        properties.getJsonSerialiserClass(),
                        properties.getJsonSerialiserModules(),
                        properties.getStrictJson()
                );
            }
            return _self();
        }

        public B storeProperties(final String propertiesPath) {
            return storeProperties(null != propertiesPath ? StoreProperties.loadStoreProperties(propertiesPath) : null);
        }

        public B storeProperties(final Path propertiesPath) {
            if (null == propertiesPath) {
                properties = null;
            } else {
                storeProperties(StoreProperties.loadStoreProperties(propertiesPath));
            }
            return _self();
        }

        public B storeProperties(final InputStream propertiesStream) {
            if (null == propertiesStream) {
                properties = null;
            } else {
                storeProperties(StoreProperties.loadStoreProperties(propertiesStream));
            }
            return _self();
        }

        public B storeProperties(final URI propertiesURI) {
            if (null != propertiesURI) {
                try {
                    storeProperties(StreamUtil.openStream(propertiesURI));
                } catch (final IOException e) {
                    throw new SchemaException("Unable to read storeProperties from URI: " + propertiesURI, e);
                }
            }

            return _self();
        }

        public B addStoreProperties(final Properties properties) {
            if (null != properties) {
                addStoreProperties(StoreProperties.loadStoreProperties(properties));
            }
            return _self();
        }

        public B addStoreProperties(final StoreProperties updateProperties) {
            if (null != updateProperties) {
                if (null == this.properties) {
                    storeProperties(updateProperties);
                } else {
                    this.properties.merge(updateProperties);
                }
            }
            return _self();
        }

        public B addStoreProperties(final String updatePropertiesPath) {
            if (null != updatePropertiesPath) {
                addStoreProperties(StoreProperties.loadStoreProperties(updatePropertiesPath));
            }
            return _self();
        }

        public B addStoreProperties(final Path updatePropertiesPath) {
            if (null != updatePropertiesPath) {
                addStoreProperties(StoreProperties.loadStoreProperties(updatePropertiesPath));
            }
            return _self();
        }

        public B addStoreProperties(final InputStream updatePropertiesStream) {
            if (null != updatePropertiesStream) {
                addStoreProperties(StoreProperties.loadStoreProperties(updatePropertiesStream));
            }
            return _self();
        }

        public B addStoreProperties(final URI updatePropertiesURI) {
            if (null != updatePropertiesURI) {
                try {
                    addStoreProperties(StreamUtil.openStream(updatePropertiesURI));
                } catch (final IOException e) {
                    throw new SchemaException("Unable to read storeProperties from URI: " + updatePropertiesURI, e);
                }
            }
            return _self();
        }

        // Json config builder
        public B json(final Path path) {
            try {
                return json(null != path ? Files.readAllBytes(path) : null);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to read config from path: " + path, e);
            }
        }

        public B json(final URI uri) {
            try {
                json(null != uri ? StreamUtil.openStream(uri) : null);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to read config from uri: " + uri, e);
            }

            return _self();
        }

        public B json(final InputStream stream) {
            try {
                json(null != stream ? IOUtils.toByteArray(stream) : null);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to read config from input stream", e);
            }

            return _self();
        }

        public B json(final byte[] bytes) {
            if (null != bytes) {
                try {
                    merge(JSONSerialiser.deserialise(bytes, Config.class));
                } catch (final IOException e) {
                    throw new IllegalArgumentException("Unable to deserialise config", e);
                }
            }
            return _self();
        }

        // Merge configs
        public B merge(final Config config) {
            if (null != config) {
                if (null != this.config.getId()) {
                    this.config.setId(config.getId());
                }
                if (null != this.config.getDescription()) {
                    this.config.setDescription(config.getDescription());
                }
                config.getHooks().forEach(hook -> this.config.addHook(hook));
                this.config.getProperties().merge(config.getProperties());
                this.config.getOperationHandlers().putAll(config.getOperationHandlers());
            }
            return _self();
        }

        public B merge(final String uri) {
            if (null != uri) {
                merge(Paths.get(uri));
            }
            return _self();
        }

        public B merge(final Path path) {
            if (null != path) {
                try {
                    merge(JSONSerialiser.deserialise(null != path ?
                                    Files.readAllBytes(path) : null,
                            config.getClass()));
                } catch (final IOException e) {
                    throw new IllegalArgumentException("Unable to read graph " +
                            "config from path: " + path, e);
                }
            }
            return _self();
        }

        public B merge(final InputStream stream) {
            try {
                merge(JSONSerialiser.deserialise(null != stream ?
                                IOUtils.toByteArray(stream) : null,
                        config.getClass()));
            } catch (
                    final IOException e) {
                throw new IllegalArgumentException("Unable to read graph config from input stream", e);
            }
            return _self();
        }

        // Hooks
        public B addHooks(final Path hooksPath) {
            if (null == hooksPath || !hooksPath.toFile().exists()) {
                throw new IllegalArgumentException("Unable to find graph hooks file: " + hooksPath);
            }
            final Hook[] hooks;
            try {
                hooks =
                        JSONSerialiser.deserialise(FileUtils.readFileToByteArray(hooksPath.toFile()), Hook[].class);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to load graph hooks file: " + hooksPath, e);
            }
            return addHooks(hooks);
        }

        public B addHook(final Path hookPath) {
            if (null == hookPath || !hookPath.toFile().exists()) {
                throw new IllegalArgumentException("Unable to find graph hook file: " + hookPath);
            }

            final Hook hook;
            try {
                hook =
                        JSONSerialiser.deserialise(FileUtils.readFileToByteArray(hookPath.toFile()), Hook.class);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to load graph hook file: " + hookPath, e);
            }
            return addHook(hook);
        }

        public B addHook(final Hook hook) {
            if (null != hook) {
                this.hooks.add(hook);
            }
            return _self();
        }

        public B addHooks(final Hook... hooks) {
            if (null != hooks) {
                this.hooks.addAll(Arrays.asList(hooks));
            }
            return _self();
        }

        public conf build() {
            return _getConf();
        }

        @Override
        public conf _getConf() {
            return config;
        }

        @Override
        public B _self() {
            return (B) this;
        }
    }
}
