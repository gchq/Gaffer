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
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is required by the Parquet reader to represent nested groups in the Parquet schema. It does not do much
 * other than act as a proxy to the leaf nodes of the Parquet schema which should be primitive types.
 */
public class BypassGroupConverter extends GroupConverter {

    private final String[] columnPath;
    private final Map<Integer, Converter> fieldToConverter;
    private final Map<String, Object[]> parquetColumnToObject;
    private final int fieldCount;

    public BypassGroupConverter(final Map<String, Object[]> parquetColumnToObject, final GroupType type, final String[] columnPath) {
        super();
        this.parquetColumnToObject = parquetColumnToObject;
        this.columnPath = columnPath;
        this.fieldCount = type.getFieldCount();
        this.fieldToConverter = buildFieldToConverter(type);
    }

    private Map<Integer, Converter> buildFieldToConverter(final GroupType schema) {
        final Map<Integer, Converter> fieldToConverter = new HashMap<>(fieldCount);
        int i = 0;
        for (final Type field : schema.getFields()) {
            final String[] newColumnPath = new String[columnPath.length + 1];
            int j = 0;
            for (final String part : columnPath) {
                newColumnPath[j] = part;
                j++;
            }
            newColumnPath[j] = field.getName();
            if (field.isPrimitive()) {
                fieldToConverter.put(i, new PrimitiveConverter(parquetColumnToObject, field.asPrimitiveType().getPrimitiveTypeName().javaType.getSimpleName(), newColumnPath, field.getOriginalType()));
            } else {
                fieldToConverter.put(i, new BypassGroupConverter(parquetColumnToObject, field.asGroupType(), newColumnPath));
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
    }

    @Override
    public void end() {

    }
}
