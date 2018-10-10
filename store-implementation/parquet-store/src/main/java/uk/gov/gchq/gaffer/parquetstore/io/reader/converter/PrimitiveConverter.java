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

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;

import java.lang.reflect.Array;
import java.util.Map;

/**
 * This class is used to add values from a single Parquet column to the 'parquetColumnToObject' variable which can then
 * be used by the {@link GafferElementConverter} to materialise the {@link uk.gov.gchq.gaffer.data.element.Element}.
 */
public class PrimitiveConverter extends org.apache.parquet.io.api.PrimitiveConverter {
    private final Map<String, Object[]> parquetColumnToObject;
    private final String column;
    private Dictionary dictionary;
    private final String expectedType;
    private final String originalType;

    public PrimitiveConverter(final Map<String, Object[]> parquetColumnToObject,
                              final String expectedType,
                              final String[] columnPath,
                              final OriginalType originalType) {
        super();
        this.parquetColumnToObject = parquetColumnToObject;
        this.expectedType = expectedType;
        this.column = String.join(".", columnPath);
        if (null != originalType) {
            this.originalType = originalType.name();
        } else {
            this.originalType = null;
        }
    }

    @Override
    public boolean isPrimitive() {
        return super.isPrimitive();
    }

    @Override
    public org.apache.parquet.io.api.PrimitiveConverter asPrimitiveConverter() {
        return super.asPrimitiveConverter();
    }

    @Override
    public boolean hasDictionarySupport() {
        return super.hasDictionarySupport();
    }

    @Override
    public void setDictionary(final Dictionary dictionary) {
        this.dictionary = dictionary;
    }

    @Override
    public void addValueFromDictionary(final int dictionaryId) {
        switch (expectedType) {
            case "Binary":  final Binary binary = this.dictionary.decodeToBinary(dictionaryId);
                            addBinary(binary);
                            break;
            case "boolean": final boolean bool = this.dictionary.decodeToBoolean(dictionaryId);
                            addBoolean(bool);
                            break;
            case "double":  final double aDouble = this.dictionary.decodeToDouble(dictionaryId);
                            addDouble(aDouble);
                            break;
            case "float":   final float aFloat = this.dictionary.decodeToFloat(dictionaryId);
                            addFloat(aFloat);
                            break;
            case "int":     final int anInt = this.dictionary.decodeToInt(dictionaryId);
                            addInt(anInt);
                            break;
            case "long":    final long aLong = this.dictionary.decodeToLong(dictionaryId);
                            addLong(aLong);
                            break;
            default:        break;
        }
    }

    @Override
    public void addBinary(final Binary value) {
        if ("UTF8".equals(originalType)) {
            addObject(value.toStringUsingUTF8());
        } else {
            addObject(value.getBytes());
        }
    }

    @Override
    public void addBoolean(final boolean value) {
        addObject(value);
    }

    @Override
    public void addDouble(final double value) {
        addObject(value);
    }

    @Override
    public void addFloat(final float value) {
        addObject(value);
    }

    @Override
    public void addInt(final int value) {
        addObject(value);
    }

    @Override
    public void addLong(final long value) {
        addObject(value);
    }

    private <T extends Object> void addObject(final T object) {
        final Object[] currentArray = parquetColumnToObject.getOrDefault(column, null);
        final Object[] newArray;
        if (null == currentArray) {
            newArray = (T[]) Array.newInstance(object.getClass(), 1);
            newArray[0] = object;
        } else {
            newArray = (T[]) Array.newInstance(object.getClass(), currentArray.length + 1);
            System.arraycopy(currentArray, 0, newArray, 0, currentArray.length);
            newArray[newArray.length - 1] = object;
        }
        parquetColumnToObject.put(column, newArray);
    }
}
