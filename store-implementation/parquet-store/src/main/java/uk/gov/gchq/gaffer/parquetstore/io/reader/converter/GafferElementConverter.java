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
package uk.gov.gchq.gaffer.parquetstore.io.reader.converter;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is used by the {@link uk.gov.gchq.gaffer.parquetstore.io.reader.ElementRecordMaterialiser} to materialise
 * each element based on the objects added to the 'parquetColumnToObject' field by the {@link PrimitiveConverter}'s.
 */
public class GafferElementConverter extends GroupConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(GafferElementConverter.class);
    private final boolean isEntity;
    private final GafferGroupObjectConverter gafferGroupObjectConverter;
    private final Map<Integer, Converter> fieldToConverter;
    private final Map<String, Object[]> parquetColumnToObject;
    private final int fieldCount;
    private Element currentRecord = null;

    public GafferElementConverter(final boolean isEntity, final MessageType schema, final GafferGroupObjectConverter gafferGroupObjectConverter) {
        super();
        this.isEntity = isEntity;
        this.parquetColumnToObject = new HashMap<>(schema.getFieldCount());
        this.gafferGroupObjectConverter = gafferGroupObjectConverter;
        this.fieldCount = schema.getFieldCount();
        this.fieldToConverter = buildFieldToConverter(schema);
    }

    private Map<Integer, Converter> buildFieldToConverter(final MessageType schema) {
        final Map<Integer, Converter> fieldToConverter = new HashMap<>(fieldCount);
        int i = 0;
        for (final Type field : schema.getFields()) {
            if (field.isPrimitive()) {
                fieldToConverter.put(i, new PrimitiveConverter(parquetColumnToObject, field.asPrimitiveType().getPrimitiveTypeName().javaType.getSimpleName(), new String[]{field.getName()}, field.getOriginalType()));
            } else {
                fieldToConverter.put(i, new BypassGroupConverter(parquetColumnToObject, field.asGroupType(), new String[]{field.getName()}));
            }
            i++;
        }
        return fieldToConverter;
    }

    @Override
    public Converter getConverter(final int fieldIndex) {
        return fieldToConverter.get(fieldIndex);
    }

    @Override
    public void start() {
        parquetColumnToObject.clear();
    }

    @Override
    public void end() {
        try {
            currentRecord = gafferGroupObjectConverter.buildElementFromParquetObjects(parquetColumnToObject, isEntity);
        } catch (final SerialisationException e) {
            LOGGER.warn("Failed to build the Element, skipping this Element {}", parquetColumnToObject);
        }
    }

    public Element getCurrentRecord() {
        return this.currentRecord;
    }
}
