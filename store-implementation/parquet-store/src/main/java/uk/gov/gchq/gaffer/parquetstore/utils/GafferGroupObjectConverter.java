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
    private final HashMap<String, String> columnToSerialiserName;
    private final HashMap<String, Serialiser> serialiserNameToSerialiser;
    private final HashMap<String, String[]> columnToPaths;
    private final String avroSchema;


    public GafferGroupObjectConverter(final HashMap<String, String> columnToSerialiserName,
                                      final HashMap<String, Serialiser> serialiserNameToSerialiser,
                                      final HashMap<String, String[]> columnToPaths,
                                      final String avroSchema) throws SerialisationException {
        this.serialiserNameToSerialiser = serialiserNameToSerialiser;
        this.columnToSerialiserName = columnToSerialiserName;
        this.columnToPaths = columnToPaths;
        this.avroSchema = avroSchema;
    }

    public Object[] gafferObjectToParquetObjects(final String gafferColumn, final Object value) throws SerialisationException {
        LOGGER.debug(this.columnToSerialiserName.get(gafferColumn));
        final Serialiser serialiser = this.serialiserNameToSerialiser.get(this.columnToSerialiserName.get(gafferColumn));
        if (serialiser instanceof ParquetSerialiser) {
            return (Object[]) serialiser.serialise(value);
        } else if (serialiser != null) {
            return new Object[]{serialiser.serialise(value)};
        } else {
            return new Object[]{value};
        }
    }

    public Object parquetObjectsToGafferObject(final String gafferColumn, final Object[] value) throws SerialisationException {
        final Serialiser serialiser = this.serialiserNameToSerialiser.get(this.columnToSerialiserName.get(gafferColumn));
        LOGGER.debug("using serialiser " + serialiser.getClass().getCanonicalName() + " to convert parquet objects to gaffer object");
        if (serialiser instanceof ParquetSerialiser) {
            return ((ParquetSerialiser) serialiser).deserialise(value);
        } else {
            if (value[0] == null) {
                return null;
            } else if (value[0] instanceof byte[]) {
                return serialiser.deserialise(value[0]);
            } else {
                throw new SerialisationException("Can not deserialise object");
            }
        }
    }

    // This will take a gafferObject and add the required Objects as specified by the avroSchema
    public void addGafferObjectToGenericRecord(final String column, final Object object, final GenericRecordBuilder recordBuilder) throws SerialisationException {
        Iterator<Object> parquetObjects = Arrays.asList(this.gafferObjectToParquetObjects(column, object)).iterator();
        final HashMap<String, Object> records = new HashMap<>();
        final String[] paths = this.columnToPaths.get(column);
        if (paths != null) {
            if (paths[0].contains(".")) {
                final String path = paths[0].substring(0, paths[0].indexOf("."));
                final Schema fieldSchema = new Schema.Parser().parse(this.avroSchema).getField(path).schema();
                recusivelyGenerateAvroObjects(parquetObjects, fieldSchema, records, path);
                for (final Map.Entry<String, Object> entry : records.entrySet()) {
                    recordBuilder.set(entry.getKey(), entry.getValue());
                }
            } else {
                for (final String path : paths) {
                    final Schema fieldSchema = new Schema.Parser().parse(this.avroSchema).getField(path).schema();
                    parquetObjects = recusivelyGenerateAvroObjects(parquetObjects, fieldSchema, records, path);
                    for (final Map.Entry<String, Object> entry : records.entrySet()) {
                        recordBuilder.set(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
    }

    private Iterator<Object> recusivelyGenerateAvroObjects(final Iterator<Object> parquetObjects, final Schema fieldSchema,
                                                           final HashMap<String, Object> recordBuilder, final String fieldName) throws SerialisationException {
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
                    final HashMap<String, Object> recordBuilderAttempt = new HashMap<>();
                    final Iterator<Object> parquetObjectsCloneIter = parquetObjectsClone.iterator();
                    newParquetObjects = recusivelyGenerateAvroObjects(parquetObjectsCloneIter, innerSchema, recordBuilderAttempt, fieldName);
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
                newParquetObjects = recusivelyGenerateAvroObjects(newParquetObjects, field.schema(), nestedRecordBuilder, field.name());
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


    // This will extract the requested Gaffer Object from the Spark row
    public Object sparkRowToGafferObject(final String gafferColumn, final GenericRowWithSchema row) throws SerialisationException {
        LOGGER.trace("Starting sparkRowToGafferObject");
        LOGGER.trace("Gaffer Column: {}", gafferColumn);
        LOGGER.trace("Row: {}", row);
        LOGGER.trace("Row Schema: {}", row.schema());
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
        } else {
            LOGGER.trace("Gaffer object type: {}", gafferObject.getClass().getCanonicalName());
            LOGGER.trace("Gaffer Object: {}", gafferObject);
        }
        return gafferObject;
    }

    private void getObjectsFromNestedRow(final ArrayList<Object> objects, final GenericRowWithSchema row) {
        LOGGER.debug("{}", row);
        for (final StructField field : row.schema().fields()) {
            final Object fieldValue = row.getAs(field.name());
            LOGGER.debug("FieldValue: " + fieldValue + " class: " + fieldValue.getClass().getCanonicalName());
            if (fieldValue instanceof GenericRowWithSchema) {
                getObjectsFromNestedRow(objects, (GenericRowWithSchema) fieldValue);
            } else {
                objects.add(fieldValue);
            }
        }
    }

    // This will take the gafferObject and generate the required Objects as specified by the sparkSchema,
    // such that each element in the returned array is a column
    public void addGafferObjectToSparkRow(final String column, final Object object, final ArrayList<Object> recordBuilder, final StructType sparkSchema) throws SerialisationException {
        LOGGER.trace("Starting addGafferObjectToSparkRow");
        LOGGER.trace("Gaffer column: {}", column);
        final String[] paths = this.columnToPaths.get(column);
        if (object != null) {
            LOGGER.trace("Gaffer Object: {}", object);
            LOGGER.trace("Gaffer object type: {}", object.getClass().getCanonicalName());
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
            LOGGER.trace("Adding the following objects: {}", records);
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
