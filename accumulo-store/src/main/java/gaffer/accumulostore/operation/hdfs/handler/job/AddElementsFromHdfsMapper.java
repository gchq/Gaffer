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
package gaffer.accumulostore.operation.hdfs.handler.job;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import gaffer.accumulostore.key.AccumuloElementConverter;
import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.accumulostore.utils.Pair;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.schema.exception.SchemaException;
import gaffer.operation.simple.hdfs.handler.AddElementsFromHdfsJobFactory;
import gaffer.operation.simple.hdfs.handler.mapper.AbstractAddElementsFromHdfsMapper;
import gaffer.store.schema.StoreSchema;

public class AddElementsFromHdfsMapper<KEY_IN, VALUE_IN>
        extends AbstractAddElementsFromHdfsMapper<KEY_IN, VALUE_IN, Key, Value> {
    private AccumuloElementConverter elementConverter;

    @Override
    protected void setup(final Context context) {
        super.setup(context);

        final StoreSchema storeSchema;
        try {
            storeSchema = StoreSchema.fromJson(context.getConfiguration()
                    .get(AddElementsFromHdfsJobFactory.STORE_SCHEMA).getBytes(AccumuloStoreConstants.UTF_8_CHARSET));
        } catch (final UnsupportedEncodingException e) {
            throw new SchemaException("Unable to deserialise Store Schema from JSON");
        }

        final String converterClass = context.getConfiguration().get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS);
        try {
            final Class<?> elementConverterClass = Class.forName(converterClass);
            elementConverter = (AccumuloElementConverter) elementConverterClass.getConstructor(StoreSchema.class)
                    .newInstance(storeSchema);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new IllegalArgumentException("Element converter could not be created: " + converterClass, e);
        }
    }

    @Override
    protected void map(final Element element, final Context context) throws IOException, InterruptedException {
        final Pair<Key> keyPair;
        try {
            keyPair = elementConverter.getKeysFromElement(element);
        } catch (final AccumuloElementConversionException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }

        final Value value;
        try {
            value = elementConverter.getValueFromElement(element);
        } catch (final AccumuloElementConversionException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }

        context.write(keyPair.getFirst(), value);
        if (keyPair.getSecond() != null) {
            context.write(keyPair.getSecond(), value);
        }
        context.getCounter("Bulk import", element.getClass().getSimpleName() + " count").increment(1L);
    }
}
