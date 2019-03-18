/*
 * Copyright 2017-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.graph.util;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.gaffer.graph.hook.GraphHookPath;
import uk.gov.gchq.gaffer.graph.schema.Schema;
import uk.gov.gchq.gaffer.graph.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.graph.schema.SchemaOptimiser;
import uk.gov.gchq.gaffer.graph.schema.TypeDefinition;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.store.util.Config;
import uk.gov.gchq.koryphe.ValidationResult;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code GraphConfig} contains configuration for Graphs. This configuration
 * is used along side a {@link Schema} and
 * {@link uk.gov.gchq.gaffer.store.StoreProperties} to create a {@link Graph}.
 * This configuration is made up of graph properties such as a graphId,
 * a graph {@link View} and {@link GraphHook}s.
 * To create an instance of GraphConfig you can either use the {@link GraphConfig.Builder}
 * or a json file.
 * If you wish to write a GraphHook in a separate json file and include it, you
 * can do it by using the {@link GraphHookPath} GraphHook and setting the path field within.
 *
 * @see GraphConfig.Builder
 */
@JsonPropertyOrder(value = {"description", "graphId"}, alphabetic = true)
public class GraphConfig extends Config {
    // Keeping the view as json enforces a new instance of View is created
    // every time it is used.
    /**
     * The schema - contains the type of {@link uk.gov.gchq.gaffer.data.element.Element}s
     * to be stored and how to aggregate the elements.
     */
    private Schema schema;
    /**
     * The original schema containing all of the original descriptions and parent groups.
     */
    private Schema originalSchema;
    private SchemaOptimiser schemaOptimiser;
    private Class<? extends Serialiser> requiredParentSerialiserClass;
    private byte[] view;

    public GraphConfig() {
    }

    public GraphConfig(final String graphId) {
        setId(graphId);
    }

    public String getGraphId() {
        return getId();
    }

    public void setGraphId(final String graphId) {
        setId(graphId);
    }

    public View getView() {
        return null != view ? View.fromJson(view) : null;
    }

    public void setView(final View view) {
        this.view = null != view ? view.toCompactJson() : null;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("graphId", getId())
                .append("view", getView())
                .append("hooks", getHooks())
                .toString();
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(final Schema schema) {
        this.schema = schema;
    }

    public SchemaOptimiser getSchemaOptimiser() {
        return schemaOptimiser;
    }

    public void setSchemaOptimiser(final SchemaOptimiser schemaOptimiser) {
        this.schemaOptimiser = schemaOptimiser;
    }

    public Schema getOriginalSchema() {
        return originalSchema;
    }

    public void setOriginalSchema(final Schema originalSchema) {
        this.originalSchema = originalSchema;
    }

    protected HashMap<String, SchemaElementDefinition> getSchemaElements() {
        final HashMap<String, SchemaElementDefinition> schemaElements = new HashMap<>();
        schemaElements.putAll(getSchema().getEdges());
        schemaElements.putAll(getSchema().getEntities());
        return schemaElements;
    }

    /**
     * Throws a {@link SchemaException} if the Vertex Serialiser is
     * inconsistent.
     */
    public void validateConsistentVertex() {
        if (null != getSchema().getVertexSerialiser() && !getSchema().getVertexSerialiser()
                .isConsistent()) {
            throw new SchemaException("Vertex serialiser is inconsistent. This store requires vertices to be serialised in a consistent way.");
        }
    }

    public void validateSchemas() {
        final ValidationResult validationResult = new ValidationResult();
        if (null == schema) {
            validationResult.addError("Schema is missing");
        } else {
            validationResult.add(schema.validate());

            getSchemaElements().forEach((key, value) -> value
                    .getProperties()
                    .forEach(propertyName -> {
                        final Class propertyClass = value
                                .getPropertyClass(propertyName);
                        final Serialiser serialisation = value
                                .getPropertyTypeDef(propertyName)
                                .getSerialiser();

                        if (null == serialisation) {
                            validationResult.addError(
                                    String.format("Could not find a serialiser for property '%s' in the group '%s'.", propertyName, key));
                        } else if (!serialisation.canHandle(propertyClass)) {
                            validationResult.addError(String.format("Schema serialiser (%s) for property '%s' in the group '%s' cannot handle property found in the schema", serialisation
                                    .getClass()
                                    .getName(), propertyName, key));
                        }
                    }));

            validateSchema(validationResult, getSchema().getVertexSerialiser());

            getSchema().getTypes()
                    .forEach((k, v) -> validateSchema(validationResult, v.getSerialiser()));
        }

        if (!validationResult.isValid()) {
            throw new SchemaException("Schema is not valid. "
                    + validationResult.getErrorString());
        }
    }

    /**
     * Ensures all identifier and property values are populated on an element by
     * triggering getters on the element for
     * all identifier and properties in the {@link Schema} forcing a lazy
     * element to load all of its values.
     *
     * @param lazyElement the lazy element
     * @return the fully populated unwrapped element
     */
    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
            justification = "Getters are called to trigger the loading data")
    public Element populateElement(final Element lazyElement) {
        final SchemaElementDefinition elementDefinition = getSchema().getElement(
                lazyElement.getGroup());
        if (null != elementDefinition) {
            for (final IdentifierType identifierType : elementDefinition.getIdentifiers()) {
                lazyElement.getIdentifier(identifierType);
            }

            for (final String propertyName : elementDefinition.getProperties()) {
                lazyElement.getProperty(propertyName);
            }
        }

        return lazyElement.getElement();
    }

    public void validateSchemaElementDefinition(final Map.Entry<String,
            SchemaElementDefinition> schemaElementDefinitionEntry, final ValidationResult validationResult) {
        schemaElementDefinitionEntry.getValue()
                .getProperties()
                .forEach(propertyName -> {
                    final Class propertyClass = schemaElementDefinitionEntry.getValue().getPropertyClass(propertyName);
                    final Serialiser serialisation = schemaElementDefinitionEntry.getValue().getPropertyTypeDef(propertyName).getSerialiser();

                    if (null == serialisation) {
                        validationResult.addError(
                                String.format("Could not find a serialiser for property '%s' in the group '%s'.", propertyName, schemaElementDefinitionEntry.getKey()));
                    } else if (!serialisation.canHandle(propertyClass)) {
                        validationResult.addError(String.format("Schema serialiser (%s) for property '%s' in the group '%s' cannot handle property found in the schema",
                                serialisation.getClass().getName(), propertyName, schemaElementDefinitionEntry.getKey()));
                    }
                });
    }

    /**
     * Ensures that each of the GroupBy properties in the {@link
     * SchemaElementDefinition} is consistent,
     * otherwise an error is added to the {@link ValidationResult}.
     *
     * @param schemaElementDefinitionEntry A map of SchemaElementDefinitions
     * @param validationResult             The validation result
     */
    public void validateConsistentGroupByProperties(final Map.Entry<String,
            SchemaElementDefinition> schemaElementDefinitionEntry, final ValidationResult validationResult) {
        for (final String property : schemaElementDefinitionEntry.getValue()
                .getGroupBy()) {
            final TypeDefinition propertyTypeDef = schemaElementDefinitionEntry.getValue()
                    .getPropertyTypeDef(property);
            if (null != propertyTypeDef) {
                final Serialiser serialiser = propertyTypeDef.getSerialiser();
                if (null != serialiser && !serialiser.isConsistent()) {
                    validationResult.addError("Serialiser for groupBy property: " + property
                            + " is inconsistent. This store requires all groupBy property serialisers to be consistent. Serialiser "
                            + serialiser.getClass().getName() + " is not consistent.");
                }
            }
        }
    }

    public void validateSchema(final ValidationResult validationResult,
                               final Serialiser serialiser) {
        if ((null != serialiser) && !requiredParentSerialiserClass.isInstance(serialiser)) {
            validationResult.addError(
                    String.format("Schema serialiser (%s) is not instance of %s",
                            serialiser.getClass().getSimpleName(),
                            requiredParentSerialiserClass.getSimpleName()));
        }
    }

    public void setRequiredParentSerialiserClass(final Class<?
            extends Serialiser> requiredParentSerialiserClass) {
        this.requiredParentSerialiserClass = requiredParentSerialiserClass;
    }

    public static class Builder extends Config.BaseBuilder<GraphConfig,
            GraphConfig.Builder> {
        private GraphConfig config = new GraphConfig();
        public static final String UNABLE_TO_READ_SCHEMA_FROM_URI = "Unable to read schema from URI";
        private final List<byte[]> schemaBytesList = new ArrayList<>();
        private Schema schema;
        private List<String> parentSchemaIds;
        private boolean addToLibrary = true;

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

        public GraphConfig.Builder addParentSchemaIds(final List<String> parentSchemaIds) {
            if (null != parentSchemaIds) {
                if (null == this.parentSchemaIds) {
                    this.parentSchemaIds = new ArrayList<>(parentSchemaIds);
                } else {
                    this.parentSchemaIds.addAll(parentSchemaIds);
                }
            }
            return this;
        }

        public GraphConfig.Builder addParentSchemaIds(final String... parentSchemaIds) {
            if (null != parentSchemaIds) {
                if (null == this.parentSchemaIds) {
                    this.parentSchemaIds = Lists.newArrayList(parentSchemaIds);
                } else {
                    Collections.addAll(this.parentSchemaIds, parentSchemaIds);
                }
            }
            return this;
        }

        public GraphConfig.Builder addSchemas(final Schema... schemaModules) {
            if (null != schemaModules) {
                for (final Schema schemaModule : schemaModules) {
                    addSchema(schemaModule);
                }
            }
            return this;
        }

        public GraphConfig.Builder addSchemas(final InputStream... schemaStreams) {
            if (null != schemaStreams) {
                try {
                    for (final InputStream schemaStream : schemaStreams) {
                        addSchema(schemaStream);
                    }
                } finally {
                    for (final InputStream schemaModule : schemaStreams) {
                        CloseableUtil.close(schemaModule);
                    }
                }
            }
            return this;
        }

        public GraphConfig.Builder addSchemas(final Path... schemaPaths) {
            if (null != schemaPaths) {
                for (final Path schemaPath : schemaPaths) {
                    addSchema(schemaPath);
                }
            }
            return this;
        }

        public GraphConfig.Builder addSchemas(final byte[]... schemaBytesArray) {
            if (null != schemaBytesArray) {
                for (final byte[] schemaBytes : schemaBytesArray) {
                    addSchema(schemaBytes);
                }
            }
            return this;
        }

        public GraphConfig.Builder addSchema(final Schema schemaModule) {
            if (null != schemaModule) {
                if (null != schema) {
                    schema = new Schema.Builder()
                            .merge(schema)
                            .merge(schemaModule)
                            .build();
                } else {
                    schema = schemaModule;
                }
            }
            return this;
        }

        public GraphConfig.Builder addSchema(final InputStream schemaStream) {
            if (null != schemaStream) {
                try {
                    addSchema(IOUtils.toByteArray(schemaStream));
                } catch (final IOException e) {
                    throw new SchemaException("Unable to read schema from input stream", e);
                } finally {
                    CloseableUtil.close(schemaStream);
                }
            }
            return this;
        }

        public GraphConfig.Builder addSchema(final URI schemaURI) {
            if (null != schemaURI) {
                try {
                    addSchema(StreamUtil.openStream(schemaURI));
                } catch (final IOException e) {
                    throw new SchemaException(UNABLE_TO_READ_SCHEMA_FROM_URI, e);
                }
            }
            return this;
        }

        public GraphConfig.Builder addSchemas(final URI... schemaURI) {
            if (null != schemaURI) {
                try {
                    addSchemas(StreamUtil.openStreams(schemaURI));
                } catch (final IOException e) {
                    throw new SchemaException(UNABLE_TO_READ_SCHEMA_FROM_URI, e);
                }
            }
            return this;
        }

        public GraphConfig.Builder addSchema(final Path schemaPath) {
            if (null != schemaPath) {
                try {
                    if (Files.isDirectory(schemaPath)) {
                        for (final Path path : Files.newDirectoryStream(schemaPath)) {
                            addSchema(path);
                        }
                    } else {
                        addSchema(Files.readAllBytes(schemaPath));
                    }
                } catch (final IOException e) {
                    throw new SchemaException("Unable to read schema from path: " + schemaPath, e);
                }
            }
            return this;
        }

        public GraphConfig.Builder addSchema(final byte[] schemaBytes) {
            if (null != schemaBytes) {
                schemaBytesList.add(schemaBytes);
            }
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

        public Builder schema(final Schema schema) {
            config.setSchema(schema);
            return this;
        }

        public GraphConfig build() {
            return config;
        }

        @Override
        public String toString() {
            return config.toString();
        }
    }
}
