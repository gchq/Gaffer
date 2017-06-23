/*
 * Copyright 2017. Crown Copyright
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

import com.databricks.spark.avro.SchemaConverters;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.BooleanParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.ParquetSerialiser;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is responsible for converting a Gaffer {@link Schema} to an Avro {@link org.apache.avro.Schema} per group
 * and to a Spark schema (a {@link StructType} per group).
 */
public class SchemaUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaUtils.class);
    private final Schema gafferSchema;
    private final Map<String, org.apache.avro.Schema> groupToAvroSchema = new HashMap<>();
    private final Map<String, StructType> groupToSparkSchema = new HashMap<>();
    private final Map<String, Map<String, String>> groupColumnToSerialiserName = new HashMap<>();
    private final Map<String, Map<String, String[]>> groupColumnToPaths = new HashMap<>();
    private final Map<String, Serialiser> serialiserNameToSerialiser = new HashMap<>();
    private final Map<String, GafferGroupObjectConverter> groupToObjectConverter = new HashMap<>();

    public SchemaUtils(final Schema gafferSchema) {
        LOGGER.info("Instantiating the SchemaUtils class");
        LOGGER.info("The Gaffer schema is:" + gafferSchema);
        this.gafferSchema = gafferSchema;
        try {
            buildAvroSchema();
            buildSparkSchema();
            buildGroupColumnToPaths();
            buildConverters();
        } catch (final SerialisationException e) {
            throw new RuntimeException("SerialisationException building Avro and Spark schemas", e);
        }
    }

    public org.apache.avro.Schema getAvroSchema(final String group) throws SerialisationException {
        return groupToAvroSchema.get(group);
    }

    private void buildGroupColumnToPaths() throws SerialisationException {
        for (final String group : getGroups()) {
            groupColumnToPaths.put(group, _getColumnToPaths(group));
        }
    }

    /**
     * This method returns, for the provided <code>group</code>, an array of {@link String}s giving the path to the
     * column or columns associated to that group
     *
     * @param group
     * @return
     * @throws SerialisationException
     */
    public Map<String, String[]> getColumnToPaths(final String group) throws SerialisationException {
        return groupColumnToPaths.get(group);
    }

    private Map<String, String[]> _getColumnToPaths(final String group) throws SerialisationException {
        final Map<String, LinkedHashSet<String>> columnToPathSet = recursivelyGeneratePaths(null, null, getAvroSchema(group), null);
        final Map<String, String[]> columnToPaths = new HashMap<>();
        for (final Map.Entry<String, LinkedHashSet<String>> entry : columnToPathSet.entrySet()) {
            LOGGER.debug("getColumnToPaths: group=" + group + " paths={}", entry.getValue().toString());
            final String[] paths = new String[entry.getValue().size()];
            entry.getValue().toArray(paths);
            columnToPaths.put(entry.getKey(), paths);
        }
        return columnToPaths;
    }

    private HashMap<String, LinkedHashSet<String>> recursivelyGeneratePaths(final String column, final String path, final org.apache.avro.Schema avroSchema, final HashMap<String, LinkedHashSet<String>> columnToPaths) throws SerialisationException {
        String newColumn = column;
        HashMap<String, LinkedHashSet<String>> newColumnToPaths = columnToPaths;
        if (avroSchema.getType() == org.apache.avro.Schema.Type.NULL) {
            return newColumnToPaths;
        } else if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
            for (final org.apache.avro.Schema innerSchema : avroSchema.getTypes()) {
                recursivelyGeneratePaths(newColumn, path, innerSchema, newColumnToPaths);
            }
        } else if (avroSchema.getType() == org.apache.avro.Schema.Type.RECORD) {
            LOGGER.debug(avroSchema.toString(true));
            if (avroSchema.getType() != org.apache.avro.Schema.Type.NULL) {
                for (final org.apache.avro.Schema.Field field : avroSchema.getFields()) {
                    final String newPath;
                    if (path == null) {
                        newPath = field.name();
                        newColumn = field.name();

                        if (newColumn.contains(".")) {
                            newColumn = newColumn.substring(0, newColumn.indexOf("."));
                        } else if (newColumn.contains("_")) {
                            newColumn = newColumn.substring(0, newColumn.indexOf("_"));
                        }
                    } else {
                        newPath = path + "." + field.name();
                    }
                    newColumnToPaths = recursivelyGeneratePaths(newColumn, newPath, field.schema(), newColumnToPaths);
                }
            }
        } else {
            newColumnToPaths = addPathToGroupColumnToPaths(newColumn, path, newColumnToPaths);
        }
        return newColumnToPaths;
    }

    public String[] getPaths(final String group, final String column) throws SerialisationException {
        return getColumnToPaths(group).get(column);
    }

    private HashMap<String, LinkedHashSet<String>> addPathToGroupColumnToPaths(final String column, final String path, final HashMap<String, LinkedHashSet<String>> columnToPaths) {
        final LinkedHashSet<String> paths;
        final HashMap<String, LinkedHashSet<String>> newColumnToPaths;
        if (columnToPaths != null) {
            newColumnToPaths = columnToPaths;
            if (newColumnToPaths.containsKey(column)) {
                paths = newColumnToPaths.get(column);
            } else {
                paths = new LinkedHashSet<>();
            }
        } else {
            newColumnToPaths = new HashMap<>();
            paths = new LinkedHashSet<>();
        }
        paths.add(path);
        newColumnToPaths.put(column, paths);
        return newColumnToPaths;
    }

    private void buildSparkSchema() throws SerialisationException {
        for (final String group : gafferSchema.getGroups()) {
            groupToSparkSchema.put(group, getSparkSchema(group));
        }
        LOGGER.info("Created Spark schema from Gaffer schema");
        LOGGER.info("Spark schema is: {}", groupToSparkSchema);
    }

    public StructType getSparkSchema(final String group) throws SerialisationException {
        final List<org.apache.avro.Schema.Field> schemaFields = getAvroSchema(group).getFields();
        final StructField[] arr = new StructField[schemaFields.size()];
        for (int i = 0; i < schemaFields.size(); i++) {
            org.apache.avro.Schema.Field field = schemaFields.get(i);
            StructField sField = new StructField(field.name(), SchemaConverters.toSqlType(field.schema()).dataType(),
                    true, new Metadata(new scala.collection.immutable.HashMap<>()));
            arr[i] = sField;
        }
        StructType sType = new StructType(arr);
        groupToSparkSchema.put(group, sType);
        return sType;
    }

    private void buildAvroSchema() throws SerialisationException {
        for (final String group : gafferSchema.getGroups()) {
            groupToAvroSchema.put(group, buildAvroSchema(group));
        }
        LOGGER.info("Created Avro schema from Gaffer schema.");
        LOGGER.info("Avro schema is: {}", groupToAvroSchema);
    }

    private org.apache.avro.Schema buildAvroSchema(final String group) throws SerialisationException {
        LOGGER.debug("Building the Avro Schema for group {}", group);
        SchemaElementDefinition groupGafferSchema;
        final boolean isEntity = gafferSchema.getEntityGroups().contains(group);
        final StringBuilder schemaString = new StringBuilder("message Entity {\n");
        schemaString.append("required binary " + ParquetStoreConstants.GROUP + " (UTF8);\n");
        Serialiser serialiser = gafferSchema.getVertexSerialiser();
        // Check that the vertex does not get stored as nested data
        if (serialiser instanceof ParquetSerialiser &&
                ((ParquetSerialiser) serialiser).getParquetSchema("test").contains(" group ")) {
            throw new SerialisationException("Can not have a vertex that is serialised as nested data as it can not be indexed");
        }
        if (isEntity) {
            groupGafferSchema = gafferSchema.getEntity(group);
            schemaString.append(convertColumnSerialiserToParquetColumns(serialiser, ParquetStoreConstants.VERTEX)).append("\n");
            addGroupColumnToSerialiser(group, ParquetStoreConstants.VERTEX, serialiser);
        } else {
            groupGafferSchema = gafferSchema.getEdge(group);

            schemaString.append(convertColumnSerialiserToParquetColumns(serialiser, ParquetStoreConstants.SOURCE)).append("\n");
            addGroupColumnToSerialiser(group, ParquetStoreConstants.SOURCE, serialiser);

            schemaString.append(convertColumnSerialiserToParquetColumns(serialiser, ParquetStoreConstants.DESTINATION)).append("\n");
            addGroupColumnToSerialiser(group, ParquetStoreConstants.DESTINATION, serialiser);

            addGroupColumnToSerialiser(group, ParquetStoreConstants.DIRECTED, BooleanParquetSerialiser.class.getCanonicalName());
            schemaString.append(convertColumnSerialiserToParquetColumns(getSerialiser(BooleanParquetSerialiser.class.getCanonicalName()), ParquetStoreConstants.DIRECTED)).append("\n");
        }

        Map<String, String> propertyMap = groupGafferSchema.getPropertyMap();
        for (final Map.Entry<String, String> entry : propertyMap.entrySet()) {
            if (entry.getKey().contains("_")) {
                throw new SchemaException("The ParquetStore does not support properties which contain the character '_'");
            }
            TypeDefinition type = gafferSchema.getType(entry.getValue());
            addGroupColumnToSerialiser(group, entry.getKey(), type.getSerialiserClass());
            schemaString.append(convertColumnSerialiserToParquetColumns(getSerialiser(type.getSerialiserClass()), entry.getKey())).append("\n");
        }
        schemaString.append("}");
        String parquetSchema = schemaString.toString();
        LOGGER.info("Generated Parquet schema:");
        LOGGER.info(parquetSchema);
        org.apache.avro.Schema avroSchema = new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(parquetSchema));
        groupToAvroSchema.put(group, avroSchema);

        LOGGER.debug("Generated Avro schema:");
        LOGGER.debug(avroSchema.toString(true));
        LOGGER.debug("Generated the columnToPaths: {}", getColumnToPaths(group));
        LOGGER.debug("Generated the columnToSerialiser: {}", getColumnToSerialiser(group));
        LOGGER.debug("Generated the SerialiserNameToSerialiser: {}", this.serialiserNameToSerialiser);

        return avroSchema;
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
        LOGGER.debug("Added group:" + group + ",column:" + column + ",serialiserClassName:" + serialiserClassName + " to groupColumnToSerialiserName and serialiserNameToSerialiser");
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
        LOGGER.debug("Added group:" + group + ",column:" + column + ",serialiserClassName:" + serialiserClassName + " to groupColumnToSerialiserName and serialiserNameToSerialiser");
    }

    private String convertColumnSerialiserToParquetColumns(final Serialiser serialiser, final String column) {
        if (serialiser instanceof ParquetSerialiser) {
            return ((ParquetSerialiser) serialiser).getParquetSchema(column);
        } else {
            LOGGER.warn(serialiser.getClass().getCanonicalName() + " does not extend ParquetSerialiser.");
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
                serialiser = (Serialiser) Class.forName(serialiserClassName).newInstance();
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

    public Map<String, String> getColumnToSerialiser(final String group) throws SerialisationException {
        if (getAvroSchema(group) == null) {
            buildAvroSchema(group);
        }
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

    public GafferGroupObjectConverter getConverter(final String group) throws SerialisationException {
        return groupToObjectConverter.get(group);
    }

    private void buildConverters() throws SerialisationException {
        for (final String group : gafferSchema.getGroups()) {
            final GafferGroupObjectConverter converter = new GafferGroupObjectConverter(getColumnToSerialiser(group),
                    getSerialisers(), getColumnToPaths(group), getAvroSchema(group).toString());
            groupToObjectConverter.put(group, converter);
        }
    }

    public View getEmptyView() {
        final View.Builder viewBuilder = new View.Builder();
        viewBuilder.entities(getEntityGroups());
        viewBuilder.edges(getEdgeGroups());
        return viewBuilder.build();
    }
}
