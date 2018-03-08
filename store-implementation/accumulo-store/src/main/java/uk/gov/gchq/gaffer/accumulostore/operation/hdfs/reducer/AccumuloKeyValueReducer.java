/*
 * Copyright 2016-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.reducer;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.hdfs.operation.reducer.GafferReducer;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.lang.reflect.InvocationTargetException;

/**
 * <p>
 * Reducer for use in bulk import of data into Accumulo. It merges all values
 * associated to the gaffer.accumulostore.key by converting them into
 * {@link uk.gov.gchq.gaffer.data.element.Properties} and then merges those, and then
 * converts them back to an Accumulo value.
 * </p>
 * <p>
 * It contains an optimisation so that if there is only one value, we simply
 * output it rather than incurring the cost of deserialising them and then
 * reserialising them.
 * </p>
 */
public class AccumuloKeyValueReducer extends GafferReducer<Key, Value> {
    private AccumuloElementConverter elementConverter;

    @Override
    protected void setup(final Context context) {
        super.setup(context);

        try {
            elementConverter = Class
                    .forName(context.getConfiguration().get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS))
                    .asSubclass(AccumuloElementConverter.class)
                    .getConstructor(Schema.class)
                    .newInstance(schema);
        } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new IllegalArgumentException("Failed to create accumulo element converter from class "
                    + context.getConfiguration().get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS), e);
        }
    }

    @Override
    protected String getGroup(final Key key, final Value firstValue) {
        return elementConverter.getGroupFromColumnFamily(key.getColumnFamilyData().getBackingArray());
    }

    @Override
    protected Properties getValueProperties(final Key key, final Value firstValue, final String group) {
        return elementConverter.getPropertiesFromValue(group, firstValue);
    }

    @Override
    protected Value createValue(final Key key, final Value firstValue, final Properties state, final String group) {
        return elementConverter.getValueFromProperties(group, state);
    }
}
