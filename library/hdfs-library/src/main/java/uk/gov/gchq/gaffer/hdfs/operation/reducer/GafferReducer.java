/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.hdfs.operation.reducer;

import org.apache.hadoop.mapreduce.Reducer;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;

import java.io.IOException;
import java.util.Iterator;

import static uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory.SampleDataForSplitPointsJobFactory.SCHEMA;

/**
 * <p>
 * Reducer for use in bulk import of data into a Gaffer Store. It merges all values
 * associated to the KEY by converting them into
 * {@link Properties} and then merges those, and then
 * converts them back to an VALUE.
 * </p>
 * <p>
 * It contains an optimisation so that if there is only one value, we simply
 * output it rather than incurring the cost of deserialising them and then
 * reserialising them.
 * </p>
 */
public abstract class GafferReducer<KEY, VALUE> extends Reducer<KEY, VALUE, KEY, VALUE> {
    protected Schema schema;

    @Override
    protected void setup(final Context context) {
        final String schemaJson = context.getConfiguration().get(SCHEMA);
        if (null != schemaJson) {
            schema = Schema.fromJson(StringUtil.toBytes(schemaJson));
        }
    }

    protected abstract String getGroup(final KEY key, final VALUE firstValue) throws Exception;

    protected abstract Properties getValueProperties(final KEY key, final VALUE firstValue, final String group) throws Exception;

    protected abstract VALUE createValue(final KEY key, final VALUE firstValue, final Properties state, final String group) throws Exception;

    protected Schema getSchema() {
        return schema;
    }

    @Override
    protected void reduce(final KEY key, final Iterable<VALUE> values, final Context context)
            throws IOException, InterruptedException {
        final Iterator<VALUE> iter = values.iterator();
        final VALUE firstValue = iter.next();
        final boolean isMulti = iter.hasNext();

        if (isMulti) {
            reduceMultiValue(key, iter, firstValue, context);
        } else {
            context.write(key, firstValue);
        }
        context.getCounter("Bulk import", getCounterId(isMulti)).increment(1L);
    }

    protected void reduceMultiValue(final KEY key, final Iterator<VALUE> iter, final VALUE firstValue, final Context context)
            throws IOException, InterruptedException {
        String group = null;
        try {
            group = getGroup(key, firstValue);
            final SchemaElementDefinition elementDef = schema.getElement(group);
            if (elementDef.isAggregate()) {
                Properties state;
                final ElementAggregator aggregator = elementDef.getIngestAggregator();
                state = getValueProperties(key, firstValue, group);
                while (iter.hasNext()) {
                    state = aggregator.apply(state, getValueProperties(key, iter.next(), group));
                }
                context.write(key, createValue(key, firstValue, state, group));
            } else {
                // The group has aggregation disabled - so write all values out.
                context.write(key, firstValue);
                while (iter.hasNext()) {
                    context.write(key, iter.next());
                }
            }
        } catch (final Exception e) {
            if (null == group) {
                group = "UNKNOWN";
            }
            throw new RuntimeException("Failed to reduce values for group: " + group, e);
        }
    }

    protected String getCounterId(final boolean isMulti) {
        return isMulti ? ">1 value" : "Only 1 value";
    }
}
