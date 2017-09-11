/*
 * Copyright 2016-2017 Crown Copyright
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
package uk.gov.gchq.gaffer.hbasestore.operation.hdfs.mapper;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.GafferMapper;

import java.io.IOException;

public class AddElementsFromHdfsMapper<KEY_IN, VALUE_IN>
        extends GafferMapper<KEY_IN, VALUE_IN, ImmutableBytesWritable, Put> {
    private ElementSerialisation serialisation;

    @Override
    protected void setup(final Context context) {
        super.setup(context);
        serialisation = new ElementSerialisation(schema);
    }

    @Override
    protected void map(final Element element, final Context context) throws IOException, InterruptedException {
        final Pair<Put, Put> puts = serialisation.getPuts(element);
        context.write(new ImmutableBytesWritable(puts.getFirst().getRow()), puts.getFirst());
        if (null != puts.getSecond()) {
            context.write(new ImmutableBytesWritable(puts.getSecond().getRow()), puts.getSecond());
        }

        context.getCounter("Bulk import", element.getClass().getSimpleName() + " count").increment(1L);
    }
}
