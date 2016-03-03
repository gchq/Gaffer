/*
 * Copyright 2016 Crown Copyright
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

package gaffer.data.elementdefinition.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import gaffer.data.elementdefinition.ElementDefinitions;
import gaffer.data.elementdefinition.Type;
import gaffer.data.elementdefinition.TypeStore;
import gaffer.data.elementdefinition.Types;
import gaffer.data.elementdefinition.schema.exception.SchemaException;
import gaffer.exception.SerialisationException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Contains the full list of {@link gaffer.data.element.Element} types to be stored in the graph.
 * <p>
 * Each type of element should have the identifier type(s) listed and a map of property names and their corresponding types.
 * Each type can either be a full java class name or a custom type. Using custom types then allows you to specify
 * validation and aggregation for the element components.
 * <p>
 * This class must be JSON serialisable.
 * A data schema should normally be written in JSON and then it will be automatically deserialised at runtime.
 * An example of a JSON data schemas can be found in the Example module.
 *
 * @see gaffer.data.elementdefinition.schema.DataSchema.Builder
 * @see gaffer.data.elementdefinition.ElementDefinitions
 */
public class DataSchema extends ElementDefinitions<DataEntityDefinition, DataEdgeDefinition> implements TypeStore {
    private static final long serialVersionUID = -6997056863871610386L;

    /**
     * A map of custom type name to {@link Type}.
     *
     * @see gaffer.data.elementdefinition.Types
     * @see gaffer.data.elementdefinition.Type
     */
    private final Types types = new Types();

    public static DataSchema fromJson(final InputStream inputStream) throws SchemaException {
        return fromJson(inputStream, DataSchema.class);
    }

    public static DataSchema fromJson(final Path filePath) throws SchemaException {
        return fromJson(filePath, DataSchema.class);
    }

    public static DataSchema fromJson(final byte[] jsonBytes) throws SchemaException {
        return fromJson(jsonBytes, DataSchema.class);
    }

    public Types getTypes() {
        return types;
    }

    /**
     * This does not override the current types it just appends the additional types.
     *
     * @param newTypes the new types to be added.
     */
    @JsonSetter("types")
    public void addTypes(final Types newTypes) {
        types.putAll(newTypes);
    }

    @JsonIgnore
    @Override
    public Type get(final String typeName) {
        return types.get(typeName);
    }

    public void addTypesFromPath(final Path path) {
        loadTypes(path);
    }

    public void addTypesFromStream(final InputStream stream) {
        loadTypes(stream);
    }

    @Override
    public void setEdges(final Map<String, DataEdgeDefinition> edges) {
        super.setEdges(edges);
        for (DataElementDefinition def : edges.values()) {
            def.setTypeStore(this);
        }
    }

    @Override
    public void setEntities(final Map<String, DataEntityDefinition> entities) {
        super.setEntities(entities);
        for (DataElementDefinition def : entities.values()) {
            def.setTypeStore(this);
        }
    }

    @Override
    public DataElementDefinition getElement(final String group) {
        return (DataElementDefinition) super.getElement(group);
    }

    @Override
    protected void addEdge(final String group, final DataEdgeDefinition elementDef) {
        super.addEdge(group, elementDef);
        elementDef.setTypeStore(this);
    }

    @Override
    protected void addEntity(final String group, final DataEntityDefinition elementDef) {
        super.addEntity(group, elementDef);
        elementDef.setTypeStore(this);
    }

    private void loadTypes(final Path path) {
        try {
            types.putAll(JSON_SERIALISER.deserialise(Files.readAllBytes(path), Types.class));
        } catch (IOException e) {
            throw new SchemaException("Failed to load schema types from file : " + e.getMessage(), e);
        }
    }

    private void loadTypes(final InputStream stream) {
        try {
            types.putAll(JSON_SERIALISER.deserialise(stream, Types.class));
        } catch (SerialisationException e) {
            throw new SchemaException("Failed to load schema types from file : " + e.getMessage(), e);
        }
    }

    public static class Builder extends ElementDefinitions.Builder<DataEntityDefinition, DataEdgeDefinition> {
        public Builder() {
            this(new DataSchema());
        }

        public Builder(final DataSchema dataSchema) {
            super(dataSchema);
        }

        @Override
        public Builder edge(final String group, final DataEdgeDefinition edgeDef) {
            return (Builder) super.edge(group, edgeDef);
        }

        @Override
        public Builder entity(final String group, final DataEntityDefinition entityDef) {
            return (Builder) super.entity(group, entityDef);
        }

        @Override
        public DataSchema build() {
            return (DataSchema) super.build();
        }
    }
}
