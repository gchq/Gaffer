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

import gaffer.accumulostore.key.AccumuloElementConverter;
import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.data.element.Properties;
import gaffer.data.element.function.ElementAggregator;
import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.data.elementdefinition.schema.exception.SchemaException;
import gaffer.operation.simple.hdfs.handler.AddElementsFromHdfsJobFactory;
import gaffer.store.schema.StoreSchema;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;

/**
 * Reducer for use in bulk import of data into Accumulo. It merges all values
 * associated to the gaffer.accumulostore.key by converting them into
 * {@link gaffer.data.element.Properties} and then merges those, and then
 * converts them back to an Accumulo value.
 * <p>
 * It contains an optimisation so that if there is only one value, we simply
 * output it rather than incurring the cost of deserialising them and then
 * reserialising them.
 */
public class AddElementsFromHdfsReducer extends Reducer<Key, Value, Key, Value> {
    private AccumuloElementConverter elementConverter;
    private DataSchema dataSchema;

    @Override
    protected void setup(final Context context) {
        final StoreSchema storeSchema;
        try {
            dataSchema = DataSchema.fromJson(context.getConfiguration().get(AddElementsFromHdfsJobFactory.DATA_SCHEMA)
                    .getBytes(AccumuloStoreConstants.UTF_8_CHARSET));
            storeSchema = StoreSchema.fromJson(context.getConfiguration()
                    .get(AddElementsFromHdfsJobFactory.STORE_SCHEMA).getBytes(AccumuloStoreConstants.UTF_8_CHARSET));
        } catch (final UnsupportedEncodingException e) {
            throw new SchemaException("Unable to deserialise Data/Store Schema from JSON");
        }

        try {
            final Class<?> elementConverterClass = Class
                    .forName(context.getConfiguration().get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS));
            elementConverter = (AccumuloElementConverter) elementConverterClass.getConstructor(StoreSchema.class)
                    .newInstance(storeSchema);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new IllegalArgumentException("Failed to create accumulo element converter from class", e);
        }
    }

    @Override
    protected void reduce(final Key key, final Iterable<Value> values, final Context context)
            throws IOException, InterruptedException {
        final Iterator<Value> iter = values.iterator();
        final Value firstValue = iter.next();
        final boolean isMulti = iter.hasNext();

        context.write(key, reduceValue(key, isMulti, iter, firstValue));
        context.getCounter("Bulk import", getCounterId(isMulti)).increment(1L);
    }

    private Value reduceValue(final Key key, final boolean isMulti, final Iterator<Value> iter,
                              final Value firstValue) {
        return isMulti ? reduceMultiValue(key, iter, firstValue) : firstValue;
    }

    private Value reduceMultiValue(final Key key, final Iterator<Value> iter, final Value firstValue) {
        final String group;
        try {
            group = new String(key.getColumnFamilyData().getBackingArray(), AccumuloStoreConstants.UTF_8_CHARSET);
        } catch (final UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        ElementAggregator aggregator;
        Properties firstPropertySet;
        try {
            firstPropertySet = elementConverter.getPropertiesFromValue(group, firstValue);
            aggregator = dataSchema.getElement(group).getAggregator();
            aggregator.aggregate(firstPropertySet);
            while (iter.hasNext()) {
                aggregator.aggregate(elementConverter.getPropertiesFromValue(group, iter.next()));
            }
        } catch (final AccumuloElementConversionException e) {
            throw new IllegalArgumentException("Failed to get Properties from an accumulo value", e);
        }
        final Properties properties = new Properties();
        aggregator.state(properties);
        try {
            return elementConverter.getValueFromProperties(properties, group);
        } catch (final AccumuloElementConversionException e) {
            throw new IllegalArgumentException("Failed to get Properties from an accumulo value", e);
        }
    }

    private String getCounterId(final boolean isMulti) {
        return isMulti ? ">1 value" : "Only 1 value";
    }
}
