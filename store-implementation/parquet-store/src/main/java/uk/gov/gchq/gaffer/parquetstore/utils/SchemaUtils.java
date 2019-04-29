/*
 * Copyright 2017-2019. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.utils;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.serialisation.ParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.BooleanParquetSerialiser;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is responsible for converting a Gaffer {@link Schema} to a Parquet {@link MessageType} per group
 * and to a Spark schema (a {@link StructType} per group). It also provides a central place to get all the mappings from
 * Gaffer Properties to columns, aggregator's to columns, serialiser's to columns, etc.
 */
public class SchemaUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaUtils.class);
    private final Schema gafferSchema;
    private final Map<String, StructType> groupToSparkSchema = new HashMap<>();
    private final Map<String, Map<String, String>> groupColumnToSerialiserName = new HashMap<>();
    private final Map<String, Map<String, String[]>> groupColumnToPaths = new HashMap<>();
    private final Map<String, Serialiser> serialiserNameToSerialiser = new HashMap<>();
    private final Map<String, GafferGroupObjectConverter> groupToObjectConverter = new HashMap<>();
    private final Map<String, MessageType> groupToParquetSchema = new HashMap<>();

    public SchemaUtils(final Schema gafferSchema) {
        LOGGER.debug("Instantiating the SchemaUtils class");
        LOGGER.debug("The Gaffer schema is: {}", gafferSchema);
        this.gafferSchema = gafferSchema;
        try {
            buildParquetSchema();
            buildSparkSchemas();
            buildGroupColumnToPaths();
            buildConverters();
        } catch (final SerialisationException e) {
            throw new RuntimeException("SerialisationException building Parquet and Spark schemas", e);
        }
    }

    public MessageType getParquetSchema(final String group) {
        return groupToParquetSchema.get(group);
    }

    private void buildGroupColumnToPaths() {
        for (final String group : getGroups()) {
            groupColumnToPaths.put(group, _getColumnToPaths(group));
        }
    }

    public List<String> getCoreProperties(final String group) {
        final List<String> properties = new ArrayList<>();
        if (getEntityGroups().contains(group)) {
            properties.add(ParquetStore.VERTEX);
        } else {
            properties.add(ParquetStore.SOURCE);
            properties.add(ParquetStore.DESTINATION);
            properties.add(ParquetStore.DIRECTED);
        }
        return properties;
    }

    public List<String> getCorePropertiesForReversedEdges() {
        final List<String> properties = new ArrayList<>();
        properties.add(ParquetStore.DESTINATION);
        properties.add(ParquetStore.SOURCE);
        properties.add(ParquetStore.DIRECTED);
        return properties;
    }

    public List<String> columnsToSortBy(final String group, final boolean reversed) {
        final boolean isEntity = getGafferSchema().getEntityGroups().contains(group);
        final Map<String, String[]> columnToPaths = getColumnToPaths(group);
        final List<String> columnsToSortBy = new ArrayList<>();
        final Map<String, String[]> groupPaths = columnToPaths;
        if (isEntity) {
            final String[] vertexPaths = groupPaths.get(ParquetStore.VERTEX);
            columnsToSortBy.add(vertexPaths[0]);
            if (vertexPaths.length > 1) {
                for (int i = 1; i < vertexPaths.length; i++) {
                    columnsToSortBy.add(vertexPaths[i]);
                }
            }
        } else {
            final String[] srcPaths = groupPaths.get(ParquetStore.SOURCE);
            final String[] destPaths = groupPaths.get(ParquetStore.DESTINATION);
            if (!reversed) {
                columnsToSortBy.add(srcPaths[0]);
                if (srcPaths.length > 1) {
                    for (int i = 1; i < srcPaths.length; i++) {
                        columnsToSortBy.add(srcPaths[i]);
                    }
                }
                for (final String destPath : destPaths) {
                    columnsToSortBy.add(destPath);
                }
            } else {
                columnsToSortBy.add(destPaths[0]);
                if (destPaths.length > 1) {
                    for (int i = 1; i < destPaths.length; i++) {
                        columnsToSortBy.add(destPaths[i]);
                    }
                }
                for (final String srcPath : srcPaths) {
                    columnsToSortBy.add(srcPath);
                }
            }
            columnsToSortBy.add(ParquetStore.DIRECTED);
        }
        // Add group-by properties
        final List<String> groupByProperties = new ArrayList<>(gafferSchema.getElement(group).getGroupBy());
        for (final String groupByProperty : groupByProperties) {
            final String[] paths = groupPaths.get(groupByProperty);
            if (null != paths && paths.length > 0) {
                columnsToSortBy.add(paths[0]);
                if (paths.length > 1) {
                    for (int i = 1; i < paths.length; i++) {
                        columnsToSortBy.add(paths[i]);
                    }
                }
            }
        }
        LOGGER.debug("Columns to sort by for group {} and reversed {} are {}", group, reversed, columnsToSortBy);
        return columnsToSortBy;
    }

    /**
     * This method returns, for the provided {@code group}, an array of {@link String}s giving the path to the
     * column or columns associated to that group.
     *
     * @param group the group
     * @return a map from column to full paths for the given group
     */
    public Map<String, String[]> getColumnToPaths(final String group) {
        return groupColumnToPaths.get(group);
    }

    private Map<String, String[]> _getColumnToPaths(final String group) {
        final Map<String, String[]> columnToPaths = new HashMap<>();
        for (final String[] paths : getParquetSchema(group).getPaths()) {
            final String firstPath = paths[0];
            final String col;
            if (firstPath.contains("_")) {
                col = firstPath.substring(0, firstPath.indexOf("_"));
            } else {
                col = firstPath;
            }
            final String newPath;
            if (paths.length > 1) {
                newPath = String.join(".", paths);
            } else {
                newPath = firstPath;
            }
            final String[] oldPaths = columnToPaths.getOrDefault(col, null);
            if (null == oldPaths) {
                columnToPaths.put(col, new String[]{newPath});
            } else {
                final String[] newPaths = new String[oldPaths.length + 1];
                int index = 0;
                for (final String path : oldPaths) {
                    newPaths[index] = path;
                    index++;
                }
                newPaths[index] = newPath;
                columnToPaths.put(col, newPaths);
            }
        }
        return columnToPaths;
    }

    public String[] getPaths(final String group, final String column) {
        return getColumnToPaths(group).get(column);
    }

    public Map<String, GafferGroupObjectConverter> getGroupToObjectConverter() {
        return groupToObjectConverter;
    }

    private void buildSparkSchemas() {
        for (final String group : gafferSchema.getGroups()) {
            groupToSparkSchema.put(group, buildSparkSchema(group));
        }
        LOGGER.debug("Created Spark schema from Gaffer schema");
        LOGGER.debug("Spark schema is: {}", groupToSparkSchema);
    }

    public StructType buildSparkSchema(final String group) {
        final StructType sType = new ParquetToSparkSchemaConverter(false, false).convert(getParquetSchema(group));
        groupToSparkSchema.put(group, sType);
        return sType;
    }

    public StructType getSparkSchema(final String group) {
        return groupToSparkSchema.get(group);
    }

    public StructType getMergedSparkSchema(final Set<String> groups) {
        StructType merged = new StructType();
        for (final String group : groups) {
            final StructType groupSchema = getSparkSchema(group);
            for (final StructField field : groupSchema.fields()) {
                if (!merged.contains(field)) {
                    merged = merged.add(field);
                }
            }
        }
        return merged;
    }

    private void buildParquetSchema() throws SerialisationException {
        for (final String group : gafferSchema.getGroups()) {
            groupToParquetSchema.put(group, buildParquetSchema(group));
        }
    }

    private MessageType buildParquetSchema(final String group) throws SerialisationException {
        SchemaElementDefinition groupGafferSchema;
        final boolean isEntity = gafferSchema.getEntityGroups().contains(group);
        final StringBuilder schemaString = new StringBuilder("message Element {\n");
        Serialiser serialiser = gafferSchema.getVertexSerialiser();
        // Check that the vertex does not get stored as nested data
        if (serialiser instanceof ParquetSerialiser &&
                ((ParquetSerialiser) serialiser).getParquetSchema("test").contains(" group ")) {
            throw new SerialisationException("Can not have a vertex that is serialised as nested data as it can not be indexed");
        }
        if (isEntity) {
            groupGafferSchema = gafferSchema.getEntity(group);
            schemaString.append(convertColumnSerialiserToParquetColumns(serialiser, ParquetStore.VERTEX)).append("\n");
            addGroupColumnToSerialiser(group, ParquetStore.VERTEX, serialiser);
        } else {
            groupGafferSchema = gafferSchema.getEdge(group);

            schemaString.append(convertColumnSerialiserToParquetColumns(serialiser, ParquetStore.SOURCE)).append("\n");
            addGroupColumnToSerialiser(group, ParquetStore.SOURCE, serialiser);

            schemaString.append(convertColumnSerialiserToParquetColumns(serialiser, ParquetStore.DESTINATION)).append("\n");
            addGroupColumnToSerialiser(group, ParquetStore.DESTINATION, serialiser);

            addGroupColumnToSerialiser(group, ParquetStore.DIRECTED, BooleanParquetSerialiser.class.getCanonicalName());
            schemaString.append(convertColumnSerialiserToParquetColumns(getSerialiser(BooleanParquetSerialiser.class.getCanonicalName()), ParquetStore.DIRECTED)).append("\n");
        }

        Map<String, String> propertyMap = groupGafferSchema.getPropertyMap();
        for (final Map.Entry<String, String> entry : propertyMap.entrySet()) {
            if (entry.getKey().contains("_") || entry.getKey().contains(".")) {
                throw new SchemaException("The ParquetStore does not support properties which contain the characters '_' or '.'");
            }
            final TypeDefinition type = gafferSchema.getType(entry.getValue());
            addGroupColumnToSerialiser(group, entry.getKey(), type.getSerialiserClass());
            schemaString.append(convertColumnSerialiserToParquetColumns(getSerialiser(type.getSerialiserClass()), entry.getKey())).append("\n");
        }
        schemaString.append("}");
        String parquetSchemaString = schemaString.toString();
        final MessageType parquetSchema = MessageTypeParser.parseMessageType(parquetSchemaString);
        LOGGER.debug("Generated Parquet schema: " + parquetSchemaString);
        return parquetSchema;
    }

    private void addGroupColumnToSerialiser(final String group, final String column, final Serialiser serialiser) {
        final String serialiserClassName = serialiser.getClass().getCanonicalName();
        if (groupColumnToSerialiserName.containsKey(group)) {
            groupColumnToSerialiserName.get(group).put(column, serialiserClassName);
        } else {
            final LinkedHashMap<String, String> columnToSerialiser = new LinkedHashMap<>();
            columnToSerialiser.put(column, serialiserClassName);
            groupColumnToSerialiserName.put(group, columnToSerialiser);
        }
        if (serialiserNameToSerialiser.containsKey(group)) {
            if (!serialiserNameToSerialiser.containsKey(serialiserClassName)) {
                serialiserNameToSerialiser.put(serialiserClassName, serialiser);
            }
        } else {
            serialiserNameToSerialiser.put(serialiserClassName, serialiser);
        }
        LOGGER.debug("Added group:{}, column:{}, serialiserClassName:{} to groupColumnToSerialiserName and serialiserNameToSerialiser", group, column, serialiserClassName);
    }

    private void addGroupColumnToSerialiser(final String group, final String column, final String serialiserClassName) throws SerialisationException {
        getSerialiser(serialiserClassName);
        if (groupColumnToSerialiserName.containsKey(group)) {
            groupColumnToSerialiserName.get(group).put(column, serialiserClassName);
        } else {
            final LinkedHashMap<String, String> columnToSerialiser = new LinkedHashMap<>();
            columnToSerialiser.put(column, serialiserClassName);
            groupColumnToSerialiserName.put(group, columnToSerialiser);
        }
        LOGGER.debug("Added group:{}, column:{}, serialiserClassName:{} to groupColumnToSerialiserName and serialiserNameToSerialiser", group, column, serialiserClassName);
    }

    private String convertColumnSerialiserToParquetColumns(final Serialiser serialiser, final String column) {
        if (serialiser instanceof ParquetSerialiser) {
            return ((ParquetSerialiser) serialiser).getParquetSchema(column);
        } else {
            LOGGER.warn("{} does not extend ParquetSerialiser.", serialiser.getClass().getCanonicalName());
            LOGGER.warn("To get the best performance out of the ParquetStore you need to be using ParquetSerialiser classes!");
            return "optional binary " + column + ";";
        }
    }

    public Serialiser getSerialiser(final String serialiserClassName) throws SerialisationException {
        if (serialiserNameToSerialiser.containsKey(serialiserClassName)) {
            return serialiserNameToSerialiser.get(serialiserClassName);
        } else {
            final Serialiser serialiser;
            try {
                serialiser = (Serialiser) Class.forName(SimpleClassNameIdResolver.getClassName(serialiserClassName)).newInstance();
            } catch (final IllegalAccessException | InstantiationException | ClassNotFoundException e) {
                throw new SerialisationException("Failed to instantiate the serialiser: " + serialiserClassName, e);
            }
            serialiserNameToSerialiser.put(serialiserClassName, serialiser);
            return serialiser;
        }
    }

    public Map<String, Serialiser> getSerialisers() {
        return serialiserNameToSerialiser;
    }

    public Map<String, String> getColumnToSerialiser(final String group) {
        return groupColumnToSerialiserName.get(group);
    }

    public Set<String> getEntityGroups() {
        return gafferSchema.getEntityGroups();
    }

    public Set<String> getEdgeGroups() {
        return gafferSchema.getEdgeGroups();
    }

    public Set<String> getGroups() {
        return gafferSchema.getGroups();
    }

    public Schema getGafferSchema() {
        return gafferSchema;
    }

    public GafferGroupObjectConverter getConverter(final String group) {
        return groupToObjectConverter.get(group);
    }

    private void buildConverters() {
        for (final String group : gafferSchema.getGroups()) {
            final GafferGroupObjectConverter converter = new GafferGroupObjectConverter(group,
                    getCoreProperties(group),
                    getCorePropertiesForReversedEdges(),
                    getColumnToSerialiser(group), getSerialisers(),
                    getColumnToPaths(group));
            groupToObjectConverter.put(group, converter);
        }
    }
}
