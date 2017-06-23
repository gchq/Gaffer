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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.parquetstore.serialisation.ParquetSerialiser;
import uk.gov.gchq.gaffer.serialisation.Serialiser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class GafferGroupObjectConverter implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(GafferGroupObjectConverter.class);
    private static final long serialVersionUID = -8098393761925808135L;
    private final Map<String, Serialiser> serialiserNameToSerialiser;
    private final Map<String, Serialiser> columnToSerialiser;
    private final Map<String, String[]> columnToPaths;
    private final String avroSchema;


    public GafferGroupObjectConverter(final Map<String, String> columnToSerialiserName,
                                      final Map<String, Serialiser> serialiserNameToSerialiser,
                                      final Map<String, String[]> columnToPaths,
                                      final String avroSchema) throws SerialisationException {
        this.serialiserNameToSerialiser = serialiserNameToSerialiser;
        this.columnToSerialiser = new HashMap<>();
        for (final Map.Entry<String, String> entry : columnToSerialiserName.entrySet()) {
            this.columnToSerialiser.put(entry.getKey(), this.serialiserNameToSerialiser.get(entry.getValue()));
        }
        this.columnToPaths = columnToPaths;
        this.avroSchema = avroSchema;
    }

    public Object[] gafferObjectToParquetObjects(final String gafferColumn, final Object value) throws SerialisationException {
        final Serialiser serialiser = this.columnToSerialiser.get(gafferColumn);
        if (serialiser instanceof ParquetSerialiser) {
            return (Object[]) serialiser.serialise(value);
        } else if (serialiser != null) {
            return new Object[]{serialiser.serialise(value)};
        } else {
            return new Object[]{value};
        }
    }

    public Object parquetObjectsToGafferObject(final String gafferColumn, final Object[] value) throws SerialisationException {
        final Serialiser serialiser = this.columnToSerialiser.get(gafferColumn);
        if (serialiser instanceof ParquetSerialiser) {
            return ((ParquetSerialiser) serialiser).deserialise(value);
        } else {
            if (value[0] == null) {
                return null;
            } else if (value[0] instanceof byte[]) {
                return serialiser.deserialise(value[0]);
            } else {
                throw new SerialisationException("Cannot deserialise object");
            }
        }
    }

    /**
     * Converts the provided <code>object</code> to an array of Parquet objects and adds them to the provided
     * {@link GenericRecordBuilder}, as specified by the Avro schema.
     *
     * @param column the column that the object has come from
     * @param object the object to be converted
     * @param recordBuilder the {@link GenericRecordBuilder} to add the converted object to
     * @throws SerialisationException if the conversion of object to Parquet objects throws a SerialisationException
     */
    public void addGafferObjectToGenericRecord(final String column,
                                               final Object object,
                                               final GenericRecordBuilder recordBuilder) throws SerialisationException {
        Iterator<Object> parquetObjects = Arrays.asList(this.gafferObjectToParquetObjects(column, object)).iterator();
        final HashMap<String, Object> records = new HashMap<>();
        final String[] paths = this.columnToPaths.get(column);
        if (paths != null) {
            if (paths[0].contains(".")) {
                final String path = paths[0].substring(0, paths[0].indexOf("."));
                final Schema fieldSchema = new Schema.Parser().parse(this.avroSchema).getField(path).schema();
                recursivelyGenerateAvroObjects(parquetObjects, fieldSchema, records, path);
                for (final Map.Entry<String, Object> entry : records.entrySet()) {
                    recordBuilder.set(entry.getKey(), entry.getValue());
                }
            } else {
                for (final String path : paths) {
                    final Schema fieldSchema = new Schema.Parser().parse(this.avroSchema).getField(path).schema();
                    parquetObjects = recursivelyGenerateAvroObjects(parquetObjects, fieldSchema, records, path);
                    for (final Map.Entry<String, Object> entry : records.entrySet()) {
                        recordBuilder.set(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
    }

    private Iterator<Object> recursivelyGenerateAvroObjects(final Iterator<Object> parquetObjects,
                                                            final Schema fieldSchema,
                                                            final HashMap<String, Object> recordBuilder,
                                                            final String fieldName) throws SerialisationException {
        Iterator<Object> newParquetObjects = parquetObjects;
        final Schema.Type type = fieldSchema.getType();
        if (type.equals(Schema.Type.UNION)) {
            LOGGER.debug("Found a Union type Schema");
            // Loop through all types until data matches a schema
            final ArrayList<Object> parquetObjectsClone = new ArrayList<>();
            while (newParquetObjects.hasNext()) {
                parquetObjectsClone.add(newParquetObjects.next());
            }
            for (final Schema innerSchema : fieldSchema.getTypes()) {
                try {
                    final HashMap<String, Object> recordBuilderAttempt = new HashMap<>();
                    final Iterator<Object> parquetObjectsCloneIter = parquetObjectsClone.iterator();
                    newParquetObjects = recursivelyGenerateAvroObjects(parquetObjectsCloneIter, innerSchema, recordBuilderAttempt, fieldName);
                    recordBuilder.putAll(recordBuilderAttempt);
                    break;
                } catch (SerialisationException ignored) {
                    newParquetObjects = parquetObjectsClone.iterator();
                }
            }
        } else if (type.equals(Schema.Type.RECORD)) {
            LOGGER.debug("Found a Record type Schema");
            final HashMap<String, Object> nestedRecordBuilder = new HashMap<>();
            for (final Schema.Field field : fieldSchema.getFields()) {
                newParquetObjects = recursivelyGenerateAvroObjects(newParquetObjects, field.schema(), nestedRecordBuilder, field.name());
            }
            final GenericRecordBuilder nestedRecorder = new GenericRecordBuilder(fieldSchema);
            for (final Map.Entry<String, Object> entry : nestedRecordBuilder.entrySet()) {
                nestedRecorder.set(entry.getKey(), entry.getValue());
            }
            recordBuilder.put(fieldSchema.getName(), nestedRecorder.build());
        } else if (type.equals(Schema.Type.MAP)) {
            LOGGER.debug("Found a Map type Schema");
            //TODO add native compatibility for maps
            throw new SerialisationException("Objects of type Map are not yet supported");
        } else if (type.equals(Schema.Type.ARRAY)) {
            LOGGER.debug("Found an Array type Schema");
            //TODO add native compatibility for arrays
            throw new SerialisationException("Objects of type List are not yet supported");
        } else {
            LOGGER.debug("Found a Primitive type Schema: {}", fieldSchema.getType());
            // must be a primitive type
            final Object parquetObject = newParquetObjects.next();
            if (type.equals(Schema.Type.NULL) && parquetObject == null) {
                recordBuilder.put(fieldName, null);
            } else if (type.equals(Schema.Type.BOOLEAN) && parquetObject instanceof Boolean) {
                recordBuilder.put(fieldName, parquetObject);
            } else if (type.equals(Schema.Type.BYTES) && parquetObject instanceof byte[]) {
                recordBuilder.put(fieldName, parquetObject);
            } else if (type.equals(Schema.Type.DOUBLE) && parquetObject instanceof Double) {
                recordBuilder.put(fieldName, parquetObject);
            } else if (type.equals(Schema.Type.FLOAT) && parquetObject instanceof Float) {
                recordBuilder.put(fieldName, parquetObject);
            } else if (type.equals(Schema.Type.INT) && parquetObject instanceof Integer) {
                recordBuilder.put(fieldName, parquetObject);
            } else if (type.equals(Schema.Type.LONG) && parquetObject instanceof Long) {
                recordBuilder.put(fieldName, parquetObject);
            } else if (type.equals(Schema.Type.STRING) && parquetObject instanceof String) {
                recordBuilder.put(fieldName, parquetObject);
            } else {
                throw new SerialisationException("Object of type " + parquetObject.getClass() + " does not match the expected type of " + type.getName());
            }
        }
        return newParquetObjects;
    }

    /**
     * Extracts an object corresponding to column <code>gafferColumn</code> from the provided {@link GenericRowWithSchema}.
     *
     * @param gafferColumn the column to extract
     * @param row the row to extract from
     * @return the extracted {@link Object}
     * @throws SerialisationException if the conversion from Parquet objects to the original object throws a
     * {@link SerialisationException}
     */
    public Object sparkRowToGafferObject(final String gafferColumn, final GenericRowWithSchema row) throws SerialisationException {
        final ArrayList<Object> objectsList = new ArrayList<>();
        final String[] paths = this.columnToPaths.get(gafferColumn);
        if (paths[0].contains(".")) {
            final GenericRowWithSchema nestedRow = row.getAs(gafferColumn);
            if (nestedRow != null) {
                getObjectsFromNestedRow(objectsList, nestedRow);
            } else {
                objectsList.add(null);
            }
        } else {
            for (final String path : paths) {
                objectsList.add(row.getAs(path));
            }
        }
        final Object[] objects = new Object[objectsList.size()];
        objectsList.toArray(objects);
        final Object gafferObject = parquetObjectsToGafferObject(gafferColumn, objects);
        if (gafferObject == null) {
            LOGGER.debug("Failed to get the Gaffer Object from the Spark Row for the column: {}", gafferColumn);
        }
        return gafferObject;
    }

    private void getObjectsFromNestedRow(final ArrayList<Object> objects, final GenericRowWithSchema row) {
        for (final StructField field : row.schema().fields()) {
            final Object fieldValue = row.getAs(field.name());
            if (fieldValue instanceof GenericRowWithSchema) {
                getObjectsFromNestedRow(objects, (GenericRowWithSchema) fieldValue);
            } else {
                objects.add(fieldValue);
            }
        }
    }

    /**
     * Converts the provided <code>object</code> into objects as specified by the <code>sparkSchema</code>.
     *
     * @param column the column that the object has come from
     * @param object the object to be converted
     * @param recordBuilder the {@link ArrayList} to add the objects resulting from the conversion to
     * @param sparkSchema the {@link StructType} that defines the Spark schema
     * @throws SerialisationException
     */
    public void addGafferObjectToSparkRow(final String column,
                                          final Object object,
                                          final ArrayList<Object> recordBuilder,
                                          final StructType sparkSchema) throws SerialisationException {
        final String[] paths = this.columnToPaths.get(column);
        if (object != null) {
            Iterator<Object> parquetObjects = Arrays.asList(this.gafferObjectToParquetObjects(column, object)).iterator();
            final ArrayList<Object> records = new ArrayList<>();
            if (paths[0].contains(".")) {
                final Schema fieldSchema = new Schema.Parser().parse(this.avroSchema).getField(column).schema();
                final DataType sparkDataType = sparkSchema.apply(column).dataType();
                if (sparkDataType instanceof StructType) {
                    recusivelyGenerateSparkObjects(parquetObjects, fieldSchema, records, (StructType) sparkDataType);
                } else {
                    recusivelyGenerateSparkObjects(parquetObjects, fieldSchema, records, sparkSchema);
                }
            } else {
                for (final String path : paths) {
                    final Schema fieldSchema = new Schema.Parser().parse(this.avroSchema).getField(path).schema();
                    final DataType sparkDataType = sparkSchema.apply(path).dataType();
                    if (sparkDataType instanceof StructType) {
                        parquetObjects = recusivelyGenerateSparkObjects(parquetObjects, fieldSchema, records, (StructType) sparkDataType);
                    } else {
                        parquetObjects = recusivelyGenerateSparkObjects(parquetObjects, fieldSchema, records, sparkSchema);
                    }
                }
            }
            recordBuilder.addAll(records);
        } else {
            LOGGER.trace("Gaffer Object: null");
            if (paths[0].contains(".")) {
                recordBuilder.add(null);
            } else {
                Arrays.asList(paths).forEach(x -> recordBuilder.add(null));
            }
        }
    }

    private Iterator<Object> recusivelyGenerateSparkObjects(final Iterator<Object> parquetObjects, final Schema fieldSchema, final ArrayList<Object> recordBuilder, final StructType sparkSchema) throws SerialisationException {
        Iterator<Object> newParquetObjects = parquetObjects;
        LOGGER.debug(fieldSchema.toString(true));
        final Schema.Type type = fieldSchema.getType();
        if (type.equals(Schema.Type.UNION)) {
            LOGGER.debug("Found a Union type Schema");
            // loop through all types until data matches a schema
            final ArrayList<Object> parquetObjectsClone = new ArrayList<>();
            while (newParquetObjects.hasNext()) {
                parquetObjectsClone.add(newParquetObjects.next());
            }
            for (final Schema innerSchema : fieldSchema.getTypes()) {
                try {
                    final ArrayList<Object> recordBuilderAttempt = new ArrayList<>();
                    final Iterator<Object> parquetObjectsCloneIter = parquetObjectsClone.iterator();
                    newParquetObjects = recusivelyGenerateSparkObjects(parquetObjectsCloneIter, innerSchema, recordBuilderAttempt, sparkSchema);
                    recordBuilder.addAll(recordBuilderAttempt);
                    break;
                } catch (SerialisationException ignored) {
                    newParquetObjects = parquetObjectsClone.iterator();
                }
            }
        } else if (type.equals(Schema.Type.RECORD)) {
            LOGGER.debug("Found a Record type Schema");
            final ArrayList<Object> nestedRecordBuilder = new ArrayList<>();
            for (final Schema.Field field : fieldSchema.getFields()) {
                final DataType sparkDataType = sparkSchema.apply(field.name()).dataType();
                if (sparkDataType instanceof StructType) {
                    newParquetObjects = recusivelyGenerateSparkObjects(newParquetObjects, field.schema(), nestedRecordBuilder, (StructType) sparkDataType);
                } else {
                    newParquetObjects = recusivelyGenerateSparkObjects(newParquetObjects, field.schema(), nestedRecordBuilder, sparkSchema);
                }
            }
            final Object[] rowObjects = new Object[nestedRecordBuilder.size()];
            nestedRecordBuilder.toArray(rowObjects);
            recordBuilder.add(new GenericRowWithSchema(rowObjects, sparkSchema));
        } else if (type.equals(Schema.Type.MAP)) {
            //TODO add native compatibility for maps
            LOGGER.debug("Found a Map type Schema");
            throw new SerialisationException("Objects of type Map are not yet supported");
        } else if (type.equals(Schema.Type.ARRAY)) {
            //TODO add native compatibility for arrays
            LOGGER.debug("Found an Array type Schema");
            throw new SerialisationException("Objects of type List are not yet supported");
        } else {
            LOGGER.debug("Found a Primitive type Schema: {}", fieldSchema.getType());
            // must be a primitive type
            final Object parquetObject = newParquetObjects.next();
            if (type.equals(Schema.Type.NULL) && parquetObject == null) {
                recordBuilder.add(null);
            } else if (type.equals(Schema.Type.BOOLEAN) && parquetObject instanceof Boolean) {
                recordBuilder.add(parquetObject);
            } else if (type.equals(Schema.Type.BYTES) && parquetObject instanceof byte[]) {
                recordBuilder.add(parquetObject);
            } else if (type.equals(Schema.Type.DOUBLE) && parquetObject instanceof Double) {
                recordBuilder.add(parquetObject);
            } else if (type.equals(Schema.Type.FLOAT) && parquetObject instanceof Float) {
                recordBuilder.add(parquetObject);
            } else if (type.equals(Schema.Type.INT) && parquetObject instanceof Integer) {
                recordBuilder.add(parquetObject);
            } else if (type.equals(Schema.Type.LONG) && parquetObject instanceof Long) {
                recordBuilder.add(parquetObject);
            } else if (type.equals(Schema.Type.STRING) && parquetObject instanceof String) {
                recordBuilder.add(parquetObject);
            } else {
                throw new SerialisationException("Object of type " + parquetObject.getClass() + " does not match the expected type of " + type.getName());
            }
        }
        return newParquetObjects;
    }
}
