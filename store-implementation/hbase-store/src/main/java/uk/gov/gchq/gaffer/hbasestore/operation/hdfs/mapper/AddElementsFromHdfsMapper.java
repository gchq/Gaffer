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
package uk.gov.gchq.gaffer.hbasestore.operation.hdfs.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.CellCreator;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.GafferMapper;

import java.io.IOException;

public class AddElementsFromHdfsMapper<KEY_IN, VALUE_IN>
        extends GafferMapper<KEY_IN, VALUE_IN, ImmutableBytesWritable, KeyValue> {
    private ElementSerialisation serialisation;
    private CellCreator kvCreator;

    @Override
    protected void setup(final Context context) {
        super.setup(context);
        serialisation = new ElementSerialisation(schema);
        Configuration conf = context.getConfiguration();
        this.kvCreator = new CellCreator(conf);
    }

    @Override
    protected void map(final Element element, final Context context) throws IOException, InterruptedException {
        final Pair<Cell, Cell> cells = serialisation.getCells(element, kvCreator);
        context.write(new ImmutableBytesWritable(((KeyValue) cells.getFirst()).getKey()), (KeyValue) cells.getFirst());
        if (null != cells.getSecond()) {
            context.write(new ImmutableBytesWritable(((KeyValue) cells.getSecond()).getKey()), (KeyValue) cells.getSecond());
        }

        context.getCounter("Bulk import", element.getClass().getSimpleName() + " count").increment(1L);
    }
}
