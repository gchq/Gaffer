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
package uk.gov.gchq.gaffer.spark.operation.dataframe.converter.schema;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.property.Converter;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.property.impl.FreqMapConverter;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.property.impl.HyperLogLogPlusConverter;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.property.impl.datasketches.theta.UnionConverter;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Converts a Gaffer {@link Schema} to a Spark {@link StructType}. Only groups that are present in the provided
 * {@link View} are used. Properties that can either automatically be converted into a value that can be used in
 * a Spark Dataframe, or for which a {@link Converter} is provided, will be present in the {@link StructType}.
 * If the same property is present in more than one group, then it must be consistent, i.e. of the same type.
 */
public class SchemaToStructTypeConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaToStructTypeConverter.class);
    private static final List<Converter> DEFAULT_CONVERTERS = new ArrayList<>();

    static {
        DEFAULT_CONVERTERS.add(new FreqMapConverter());
        DEFAULT_CONVERTERS.add(new HyperLogLogPlusConverter());
        DEFAULT_CONVERTERS.add(new UnionConverter());
    }

    enum EntityOrEdge {
        ENTITY, EDGE
    }

    public static final String GROUP = "group";
    public static final String VERTEX_COL_NAME = "vertex";
    public static final String SRC_COL_NAME = "src";
    public static final String DST_COL_NAME = "dst";

    private final Schema schema;
    private final List<Converter> converters = new ArrayList<>();
    private final LinkedHashSet<String> groups = new LinkedHashSet<>();
    private StructType structType;
    private final Map<String, EntityOrEdge> entityOrEdgeByGroup = new HashMap<>();
    private final LinkedHashSet<String> usedProperties = new LinkedHashSet<>();
    private final Map<String, Boolean> propertyNeedsConversion = new HashMap<>();
    private final Map<String, Converter> converterByProperty = new HashMap<>();
    private final Map<String, StructType> structTypeByGroup = new HashMap<>();

    public SchemaToStructTypeConverter(final Schema schema, final View view, final List<Converter> converters) {
        this.schema = schema;
        this.converters.addAll(DEFAULT_CONVERTERS);
        if (converters != null) {
            this.converters.addAll(converters);
        }
        // Extract the relevant groups from the view
        addGroups(view);
        // Build the schema
        buildSchema();
    }

    public LinkedHashSet<String> getGroups() {
        return groups;
    }

    public StructType getStructType() {
        return structType;
    }

    public LinkedHashSet<String> getUsedProperties() {
        return usedProperties;
    }

    public Map<String, Boolean> getPropertyNeedsConversion() {
        return propertyNeedsConversion;
    }

    public Map<String, Converter> getConverterByProperty() {
        return converterByProperty;
    }

    private void addGroups(final View view) {
        groups.addAll(view.getGroups());
    }

    private void buildSchema() {
        LOGGER.info("Building Spark SQL schema for groups {}", StringUtils.join(groups, ','));
        for (final String group : groups) {
            final SchemaElementDefinition elementDefn = schema.getElement(group);
            final List<StructField> structFieldList = new ArrayList<>();
            if (elementDefn instanceof SchemaEntityDefinition) {
                entityOrEdgeByGroup.put(group, EntityOrEdge.ENTITY);
                final SchemaEntityDefinition entityDefinition = (SchemaEntityDefinition) elementDefn;
                final String vertexClass = schema.getType(entityDefinition.getVertex()).getClassString();
                final DataType vertexType = getType(vertexClass);
                if (vertexType == null) {
                    throw new RuntimeException("Vertex must be a recognised type: found " + vertexClass);
                }
                LOGGER.info("Group {} is an entity group - {} is of type {}", group, VERTEX_COL_NAME, vertexType);
                structFieldList.add(new StructField(VERTEX_COL_NAME, vertexType, true, Metadata.empty()));
            } else {
                entityOrEdgeByGroup.put(group, EntityOrEdge.EDGE);
                final SchemaEdgeDefinition edgeDefinition = (SchemaEdgeDefinition) elementDefn;
                final String srcClass = schema.getType(edgeDefinition.getSource()).getClassString();
                final String dstClass = schema.getType(edgeDefinition.getDestination()).getClassString();
                final DataType srcType = getType(srcClass);
                final DataType dstType = getType(dstClass);
                if (srcType == null || dstType == null) {
                    throw new RuntimeException("Both source and destination must be recognised types: source was "
                            + srcClass + " destination was " + dstClass);
                }
                LOGGER.info("Group {} is an edge group - {} is of type {}, {} is of type {}",
                        group,
                        SRC_COL_NAME,
                        srcType,
                        DST_COL_NAME,
                        dstType);
                structFieldList.add(new StructField(SRC_COL_NAME, srcType, true, Metadata.empty()));
                structFieldList.add(new StructField(DST_COL_NAME, dstType, true, Metadata.empty()));
            }
            final Set<String> properties = elementDefn.getProperties();
            for (final String property : properties) {
                // Check if property is of a known type that can be handled by default
                final String propertyClass = elementDefn.getPropertyClass(property).getCanonicalName();
                DataType propertyType = getType(propertyClass);
                if (propertyType != null) {
                    propertyNeedsConversion.put(property, needsConversion(propertyClass));
                    structFieldList.add(new StructField(property, propertyType, true, Metadata.empty()));
                    LOGGER.info("Property {} is of type {}", property, propertyType);
                } else {
                    // Check if any of the provided converters can handle it
                    if (converters != null) {
                        for (final Converter converter : converters) {
                            if (converter.canHandle(elementDefn.getPropertyClass(property))) {
                                propertyNeedsConversion.put(property, true);
                                propertyType = converter.convertedType();
                                converterByProperty.put(property, converter);
                                structFieldList.add(new StructField(property, propertyType, true, Metadata.empty()));
                                LOGGER.info("Property {} of type {} will be converted by {} to {}",
                                        property,
                                        propertyClass,
                                        converter.getClass().getName(),
                                        propertyType);
                                break;
                            }
                        }
                        if (propertyType == null) {
                            LOGGER.warn("Ignoring property {} as it is not a recognised type and none of the provided "
                                    + "converters can handle it", property);
                        }
                    }
                }
            }
            structTypeByGroup.put(group, new StructType(structFieldList.toArray(new StructField[structFieldList.size()])));
        }
        // Create reverse map of field name to StructField
        final Map<String, Set<StructField>> fieldToStructs = new HashMap<>();
        for (final String group : groups) {
            final StructType groupSchema = structTypeByGroup.get(group);
            for (final String field : groupSchema.fieldNames()) {
                if (fieldToStructs.get(field) == null) {
                    fieldToStructs.put(field, new HashSet<StructField>());
                }
                fieldToStructs.get(field).add(groupSchema.apply(field));
            }
        }
        // Check consistency, i.e. if the same field appears in multiple groups then the types are consistent
        for (final Entry<String, Set<StructField>> entry : fieldToStructs.entrySet()) {
            final Set<StructField> schemas = entry.getValue();
            if (schemas.size() > 1) {
                throw new IllegalArgumentException("Inconsistent fields: the field "
                        + entry.getKey()
                        + " has more than one definition: "
                        + StringUtils.join(schemas, ','));
            }
        }
        // Merge schemas for groups together - fields should appear in the order the groups were provided
        final LinkedHashSet<StructField> fields = new LinkedHashSet<>();
        fields.add(new StructField(GROUP, DataTypes.StringType, false, Metadata.empty()));
        usedProperties.add(GROUP);
        for (final String group : groups) {
            final StructType groupSchema = structTypeByGroup.get(group);
            for (final String field : groupSchema.fieldNames()) {
                final StructField struct = groupSchema.apply(field);
                // Add struct to fields unless it has already been added
                if (!fields.contains(struct)) {
                    fields.add(struct);
                    usedProperties.add(field);
                }
            }
        }
        structType = new StructType(fields.toArray(new StructField[fields.size()]));
        LOGGER.info("Schema is {}", structType);
        LOGGER.debug("properties -> conversion: {}", StringUtils.join(propertyNeedsConversion.entrySet(), ','));
    }

    private static DataType getType(final String className) {
        switch (className) {
            case "java.lang.String":
                return DataTypes.StringType;
            case "java.lang.Integer":
                return DataTypes.IntegerType;
            case "java.lang.Long":
                return DataTypes.LongType;
            case "java.lang.Boolean":
                return DataTypes.BooleanType;
            case "java.lang.Double":
                return DataTypes.DoubleType;
            case "java.lang.Float":
                return DataTypes.FloatType;
            case "java.lang.Byte":
                return DataTypes.ByteType;
            case "java.lang.Short":
                return DataTypes.ShortType;
            default:
                return null;
        }
    }

    private static boolean needsConversion(final String className) {
        switch (className) {
            case "java.lang.String":
                return false;
            case "java.lang.Integer":
                return false;
            case "java.lang.Long":
                return false;
            case "java.lang.Boolean":
                return false;
            case "java.lang.Double":
                return false;
            case "java.lang.Float":
                return false;
            case "java.lang.Byte":
                return false;
            case "java.lang.Short":
                return false;
            default:
                return true;
        }
    }

}
