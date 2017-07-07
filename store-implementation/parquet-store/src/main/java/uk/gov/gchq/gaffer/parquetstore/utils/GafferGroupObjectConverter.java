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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
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


    public GafferGroupObjectConverter(final Map<String, String> columnToSerialiserName,
                                      final Map<String, Serialiser> serialiserNameToSerialiser,
                                      final Map<String, String[]> columnToPaths) throws SerialisationException {
        this.serialiserNameToSerialiser = serialiserNameToSerialiser;
        this.columnToSerialiser = new HashMap<>();
        for (final Map.Entry<String, String> entry : columnToSerialiserName.entrySet()) {
            this.columnToSerialiser.put(entry.getKey(), this.serialiserNameToSerialiser.get(entry.getValue()));
        }
        this.columnToPaths = columnToPaths;
    }

    public Object[] gafferObjectToParquetObjects(final String gafferColumn, final Object value) throws SerialisationException {
        final Serialiser serialiser = columnToSerialiser.get(gafferColumn);
        if (serialiser instanceof ParquetSerialiser) {
            return (Object[]) serialiser.serialise(value);
        } else if (serialiser != null) {
            return new Object[]{serialiser.serialise(value)};
        } else {
            return new Object[]{value};
        }
    }

    public Object[] gafferObjectToSparkObjects(final String gafferColumn, final Object value) throws SerialisationException {
        final Serialiser serialiser = columnToSerialiser.get(gafferColumn);
        if (serialiser instanceof ParquetSerialiser) {
            final Object[] serialisedObjects = (Object[]) serialiser.serialise(value);
            final Object[] sparkObjects = new Object[serialisedObjects.length];
            int index = 0;
            for (final Object obj : serialisedObjects) {
                if (obj instanceof String) {
                    sparkObjects[index] = UTF8String.fromString((String) obj);
                } else {
                    sparkObjects[index] = obj;
                }
                index++;
            }
            return sparkObjects;
        } else if (serialiser != null) {
            return new Object[]{serialiser.serialise(value)};
        } else {
            return new Object[]{value};
        }
    }

    public Object parquetObjectsToGafferObject(final String gafferColumn, final Object[] value) throws SerialisationException {
        final Serialiser serialiser = columnToSerialiser.get(gafferColumn);
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
        final String[] paths = columnToPaths.get(gafferColumn);
        if (paths[0].contains(".")) {
            final GenericRowWithSchema nestedRow = row.getAs(gafferColumn);
            if (nestedRow != null) {
                getObjectsFromNestedRow(objectsList, nestedRow);
            } else {
                objectsList.add(null);
            }
        } else {
            for (final String path : paths) {
                final Object obj = row.getAs(path);
                if (obj instanceof UTF8String) {
                    objectsList.add(obj.toString());
                } else {
                    objectsList.add(obj);
                }
            }
        }
        final Object[] objects = new Object[paths.length];
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
                if (fieldValue instanceof UTF8String) {
                    objects.add(fieldValue.toString());
                } else {
                    objects.add(fieldValue);
                }
            }
        }
    }

    public InternalRow convertElementToSparkRow(final Element e, final String[] gafferProperties,
                                                final StructType sparkSchema) throws SerialisationException {
        ArrayList<Object> outputRow = new ArrayList<>();
        outputRow.add(UTF8String.fromString(e.getGroup()));
        if (e instanceof Entity) {
            outputRow.addAll(Arrays.asList(gafferObjectToSparkObjects(ParquetStoreConstants.VERTEX, ((Entity) e).getVertex())));
        } else {
            final Edge edge = (Edge) e;
            outputRow.addAll(Arrays.asList(gafferObjectToSparkObjects(ParquetStoreConstants.SOURCE, edge.getSource())));
            outputRow.addAll(Arrays.asList(gafferObjectToSparkObjects(ParquetStoreConstants.DESTINATION, edge.getDestination())));
            outputRow.add(edge.getDirectedType().isDirected());
        }
        for (final String propName : gafferProperties) {
            addGafferObjectToSparkRow(propName, e.getProperty(propName), outputRow, sparkSchema, true);
        }
        return new GenericInternalRow(outputRow.toArray());
    }

    /**
     * Converts the provided <code>object</code> into objects as specified by the <code>sparkSchema</code>.
     *
     * @param column the column that the object has come from
     * @param object the object to be converted
     * @param recordBuilder the {@link ArrayList} to add the objects resulting from the conversion to
     * @param sparkSchema the {@link StructType} that defines the Spark schema
     * @param isInternalRow a boolean flag to indicate whether the row which the Gaffer object is to be added to is an {@link InternalRow}
     * @throws SerialisationException if the object cannot be serialised
     */
    public void addGafferObjectToSparkRow(final String column,
                                          final Object object,
                                          final ArrayList<Object> recordBuilder,
                                          final StructType sparkSchema, final boolean isInternalRow) throws SerialisationException {
        final String[] paths = columnToPaths.get(column);
        if (object != null) {
            final Iterator<Object> parquetObjects;
            if (isInternalRow) { // if it is to be used for an internal row then String classes must be as UTF8String
                parquetObjects = Arrays.asList(gafferObjectToSparkObjects(column, object)).iterator();
            } else {
                parquetObjects = Arrays.asList(gafferObjectToParquetObjects(column, object)).iterator();
            }
            final ArrayList<Object> records = new ArrayList<>();
            if (paths[0].contains(".")) { // it is a nested structure
                recusivelyGenerateSparkObjects(parquetObjects, sparkSchema.apply(column).dataType(), records, isInternalRow);
            } else {
                for (final String path : paths) {
                    recusivelyGenerateSparkObjects(parquetObjects, sparkSchema.apply(path).dataType(), records, isInternalRow);
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

    private void recusivelyGenerateSparkObjects(final Iterator<Object> parquetObjects,
                                                final DataType fieldType,
                                                final ArrayList<Object> recordBuilder,
                                                final boolean isInternalRow) throws SerialisationException {
        if (fieldType instanceof StructType) {
            final ArrayList<Object> nestedRecordBuilder = new ArrayList<>();
            for (final String field : ((StructType) fieldType).fieldNames()) {
                final DataType innerDataType = ((StructType) fieldType).apply(field).dataType();
                recusivelyGenerateSparkObjects(parquetObjects, innerDataType, nestedRecordBuilder, isInternalRow);
            }
            final Object[] rowObjects = new Object[nestedRecordBuilder.size()];
            nestedRecordBuilder.toArray(rowObjects);
            if (isInternalRow) {
                recordBuilder.add(new GenericInternalRow(rowObjects));
            } else {
                recordBuilder.add(new GenericRowWithSchema(rowObjects, (StructType) fieldType));
            }
        } else {
            // must be a primitive type
            final Object parquetObject = parquetObjects.next();
            if (isInternalRow && fieldType instanceof StringType && parquetObject instanceof String) {
                recordBuilder.add(UTF8String.fromString((String) parquetObject));
            } else {
                recordBuilder.add(parquetObject);
            }
        }
    }
}
