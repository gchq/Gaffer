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
package uk.gov.gchq.gaffer.hbasestore.operation.hdfs.reducer;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hdfs.operation.reducer.GafferReducer;

/**
 * <p>
 * Reducer for use in bulk import of data into HBase. It merges all values
 * associated to the key by converting them into
 * {@link uk.gov.gchq.gaffer.data.element.Properties} and then merges those, and then
 * converts them back to an HBase KeyValue.
 * </p>
 * <p>
 * It contains an optimisation so that if there is only one value, we simply
 * output it rather than incurring the cost of deserialising them and then
 * reserialising them.
 * </p>
 */
public class AddElementsFromHdfsReducer extends GafferReducer<ImmutableBytesWritable, KeyValue> {
    private ElementSerialisation serialisation;

    @Override
    protected void setup(final Context context) {
        super.setup(context);
        serialisation = new ElementSerialisation(schema);
    }

    @Override
    protected String getGroup(final ImmutableBytesWritable immutableBytesWritable, final KeyValue firstValue) throws SerialisationException {
        return serialisation.getGroup(firstValue);
    }


    @Override
    protected Properties getValueProperties(final ImmutableBytesWritable immutableBytesWritable, final KeyValue firstValue, final String group) throws SerialisationException {
        return serialisation.getProperties(group, firstValue);
    }

    @Override
    protected KeyValue createValue(final ImmutableBytesWritable immutableBytesWritable, final KeyValue firstValue, final Properties state, final String group) throws SerialisationException {
        return (KeyValue) CellUtil.createCell(
                CellUtil.cloneRow(firstValue),
                CellUtil.cloneFamily(firstValue),
                CellUtil.cloneQualifier(firstValue),
                firstValue.getTimestamp(),
                KeyValue.Type.Maximum,
                serialisation.getValue(group, state),
                CellUtil.getTagArray(firstValue));
    }
}
