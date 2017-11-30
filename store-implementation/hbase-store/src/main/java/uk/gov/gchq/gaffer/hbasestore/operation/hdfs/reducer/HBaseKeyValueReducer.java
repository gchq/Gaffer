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
package uk.gov.gchq.gaffer.hbasestore.operation.hdfs.reducer;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;

import static uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory.SampleDataForSplitPointsJobFactory.SCHEMA;

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
public class HBaseKeyValueReducer extends Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, KeyValue> {
    private ElementSerialisation serialisation;
    private Schema schema;

    @Override
    protected void setup(final Context context) {
        try {
            schema = Schema.fromJson(context.getConfiguration()
                    .get(SCHEMA).getBytes(CommonConstants.UTF_8));
        } catch (final UnsupportedEncodingException e) {
            throw new SchemaException("Unable to deserialise schema from JSON", e);
        }

        serialisation = new ElementSerialisation(schema);
    }

    @Override
    protected void reduce(final ImmutableBytesWritable key, final Iterable<KeyValue> values, final Context context)
            throws IOException, InterruptedException {
        final Iterator<KeyValue> iter = values.iterator();
        final KeyValue firstValue = iter.next();
        final boolean isMulti = iter.hasNext();

        if (isMulti) {
            reduceMultiValue(key, iter, firstValue, context);
        } else {
            context.write(key, firstValue);
        }
        context.getCounter("Bulk import", getCounterId(isMulti)).increment(1L);
    }

    private void reduceMultiValue(final ImmutableBytesWritable key, final Iterator<KeyValue> iter, final KeyValue firstValue, final Context context)
            throws IOException, InterruptedException {
        final String group = serialisation.getGroup(firstValue);

        final SchemaElementDefinition elementDef = schema.getElement(group);
        if (elementDef.isAggregate()) {
            Properties state;
            final ElementAggregator aggregator = elementDef.getIngestAggregator();
            state = serialisation.getProperties(group, firstValue);
            while (iter.hasNext()) {
                state = aggregator.apply(state, serialisation.getProperties(group, iter.next()));
            }
            final KeyValue aggregatedKeyValue = (KeyValue) CellUtil.createCell(
                    CellUtil.cloneRow(firstValue),
                    CellUtil.cloneFamily(firstValue),
                    CellUtil.cloneQualifier(firstValue),
                    firstValue.getTimestamp(),
                    KeyValue.Type.Maximum,
                    serialisation.getValue(group, state),
                    CellUtil.getTagArray(firstValue)
            );
            context.write(key, aggregatedKeyValue);
        } else {
            // The group has aggregation disabled - so write all values out.
            context.write(key, firstValue);
            while (iter.hasNext()) {
                context.write(key, iter.next());
            }
        }
    }

    private String getCounterId(final boolean isMulti) {
        return isMulti ? ">1 value" : "Only 1 value";
    }
}
