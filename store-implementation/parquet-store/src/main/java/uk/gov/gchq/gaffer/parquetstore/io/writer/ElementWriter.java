/*
 * Copyright 2017-2018. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.io.writer;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;

import java.util.List;
import java.util.Map;

/**
 * This class writes the Gaffer {@link uk.gov.gchq.gaffer.data.element.Element}s to Parquet files.
 */
public class ElementWriter {
    private static final String KEY_VALUE = "key_value";
    private static final String LIST = "list";

    private final RecordConsumer recordConsumer;
    private final GroupType schema;
    private final GafferGroupObjectConverter converter;

    public ElementWriter(final RecordConsumer recordConsumer, final GroupType schema,
                         final GafferGroupObjectConverter converter) {
        this.recordConsumer = recordConsumer;
        this.schema = schema;
        this.converter = converter;
    }

    public void writeElement(final Element element) throws SerialisationException {
        if (element instanceof Entity) {
            write((Entity) element);
        } else {
            write((Edge) element);
        }
    }

    public void write(final Entity entity) throws SerialisationException {
        recordConsumer.startMessage();
        final int indexPos = writeEntity(entity, schema);
        writeProperties(entity.getProperties(), schema, indexPos);
        recordConsumer.endMessage();
    }

    public void write(final Edge edge) throws SerialisationException {
        recordConsumer.startMessage();
        final int indexPos = writeEdge(edge, schema);
        writeProperties(edge.getProperties(), schema, indexPos);
        recordConsumer.endMessage();
    }

    private int writeEntity(final Entity entity, final GroupType type) throws SerialisationException {
        return writeGafferObject(ParquetStore.VERTEX, entity.getVertex(), type, 0);
    }

    private int writeEdge(final Edge edge, final GroupType type) throws SerialisationException {
        int currentFieldIndex = writeGafferObject(ParquetStore.SOURCE, edge.getSource(), type, 0);
        currentFieldIndex = writeGafferObject(ParquetStore.DESTINATION, edge.getDestination(), type, currentFieldIndex);
        recordConsumer.startField(ParquetStore.DIRECTED, currentFieldIndex);
        recordConsumer.addBoolean(edge.getDirectedType().isDirected());
        recordConsumer.endField(ParquetStore.DIRECTED, currentFieldIndex);
        return currentFieldIndex + 1;
    }

    private void writeProperties(final Properties properties, final GroupType type, final int startIndex) throws SerialisationException {
        for (int i = startIndex; i < type.getFieldCount(); i++) {
            final String fieldName = type.getFieldName(i);
            final String columnName;
            if (fieldName.contains("_")) {
                columnName = fieldName.substring(0, fieldName.indexOf("_"));
            } else {
                columnName = fieldName;
            }
            i = writeGafferObject(columnName, properties.get(columnName), type, i) - 1;
        }
    }

    private int writeGafferObject(final String gafferColumn, final Object gafferObject, final GroupType type, final int startIndex) throws SerialisationException {
        final Object[] parquetObjects = converter.gafferObjectToParquetObjects(gafferColumn, gafferObject);
        for (int i = 0; i < parquetObjects.length; i++) {
            writeObject(type.getType(startIndex + i), parquetObjects[i], startIndex + i);
        }
        return startIndex + parquetObjects.length;
    }

    private void writeObject(final Type type, final Object object, final int index) throws SerialisationException {
        if (null != object) {
            final String fieldName = type.getName();
            if (type.isPrimitive()) {
                recordConsumer.startField(fieldName, index);
                if (object instanceof Object[]) {
                    for (final Object innerObject : (Object[]) object) {
                        writePrimitive(innerObject);
                    }
                } else {
                    writePrimitive(object);
                }
                recordConsumer.endField(fieldName, index);
            } else {
                final String originalType = type.getOriginalType().name();
                if ("MAP".equals(originalType)) {
                    writeMap(fieldName, index, (Map<Object, Object>) object, type);
                } else if ("LIST".equals(originalType)) {
                    writeList(fieldName, index, object, type);
                } else {
                    throw new SerialisationException("Could not write object " + object.toString() + " with type " + type.toString());
                }
            }
        }
    }

    private void writeMap(final String fieldName, final int index, final Map<Object, Object> object, final Type type) throws SerialisationException {
        if (!object.isEmpty()) {
            recordConsumer.startField(fieldName, index);
            recordConsumer.startGroup();
            recordConsumer.startField(KEY_VALUE, 0);
            recordConsumer.startGroup();
            writeObject(type.asGroupType().getType(0).asGroupType().getType(0), object.keySet().toArray(), 0);
            writeObject(type.asGroupType().getType(0).asGroupType().getType(1), object.values().toArray(), 1);
            recordConsumer.endGroup();
            recordConsumer.endField(KEY_VALUE, 0);
            recordConsumer.endGroup();
            recordConsumer.endField(fieldName, index);
        }
    }

    private void writeList(final String fieldName, final int index, final Object object, final Type type) throws SerialisationException {

        if (object instanceof List) {
            if (!((List) object).isEmpty()) {
                recordConsumer.startField(fieldName, index);
                recordConsumer.startGroup();
                recordConsumer.startField(LIST, 0);
                recordConsumer.startGroup();
                writeObject(type.asGroupType().getType(0).asGroupType().getType(0), ((List) object).toArray(), 0);
                recordConsumer.endGroup();
                recordConsumer.endField(LIST, 0);
                recordConsumer.endGroup();
                recordConsumer.endField(fieldName, index);
            }
        } else if (object instanceof Object[]) {
            recordConsumer.startField(fieldName, index);
            recordConsumer.startGroup();
            recordConsumer.startField(LIST, 0);
            recordConsumer.startGroup();
            writeObject(type.asGroupType().getType(0).asGroupType().getType(0), object, 0);
            recordConsumer.endGroup();
            recordConsumer.endField(LIST, 0);
            recordConsumer.endGroup();
            recordConsumer.endField(fieldName, index);
        }
    }

    private void writePrimitive(final Object object) throws SerialisationException {
        if (object instanceof String) {
            recordConsumer.addBinary(Binary.fromString((String) object));
        } else if (object instanceof byte[]) {
            recordConsumer.addBinary(Binary.fromReusedByteArray((byte[]) object));
        } else if (object instanceof Long) {
            recordConsumer.addLong((long) object);
        } else if (object instanceof Integer) {
            recordConsumer.addInteger((int) object);
        } else if (object instanceof Float) {
            recordConsumer.addFloat((float) object);
        } else if (object instanceof Double) {
            recordConsumer.addDouble((double) object);
        } else if (object instanceof Boolean) {
            recordConsumer.addBoolean((boolean) object);
        } else {
            throw new SerialisationException(object.getClass().getCanonicalName() + " is not a supported primitive type");
        }
    }
}
