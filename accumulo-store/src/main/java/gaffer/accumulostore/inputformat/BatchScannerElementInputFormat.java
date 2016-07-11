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

package gaffer.accumulostore.inputformat;

import gaffer.accumulostore.key.AccumuloElementConverter;
import gaffer.accumulostore.key.AccumuloKeyPackage;
import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.commonutil.CommonConstants;
import gaffer.data.element.Element;
import gaffer.data.element.function.ElementTransformer;
import gaffer.data.elementdefinition.exception.SchemaException;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.exception.SerialisationException;
import gaffer.store.StoreException;
import gaffer.store.schema.Schema;
import geotrellis.spark.io.accumulo.BatchAccumuloInputFormat;
import geotrellis.spark.io.accumulo.MultiRangeInputSplit;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An {@link InputFormatBase} that allows the data in an Accumulo store to be read as {@link Element},
 * {@link NullWritable} pairs. This uses an external library that creates an {@link InputSplit} for each
 * tablet that contains records from the required ranges. This avoids the creation of large numbers of
 * splits when a large number of ranges are required. Instead, a single {@link BatchScanner} is created
 * per tablet.
 */
public class BatchScannerElementInputFormat extends InputFormatBase<Element, NullWritable> {

    @Override
    public List<InputSplit> getSplits(final JobContext context) throws IOException {
        return new BatchAccumuloInputFormat().getSplits(context);
    }

    @Override
    public RecordReader<Element, NullWritable> createRecordReader(final InputSplit split, final TaskAttemptContext context)
            throws IOException, InterruptedException {
        log.setLevel(getLogLevel(context));
        final Configuration conf = context.getConfiguration();
        final String keyPackageClass = conf.get(ElementInputFormat.KEY_PACKAGE);
        final Schema schema = Schema.fromJson(conf.get(ElementInputFormat.SCHEMA).getBytes(CommonConstants.UTF_8));
        final View view = View.fromJson(conf.get(ElementInputFormat.VIEW).getBytes(CommonConstants.UTF_8));
        try {
            return new BatchScannerRecordReader(keyPackageClass, schema, view);
        } catch (final StoreException | SchemaException | SerialisationException e) {
            throw new IOException("Exception creating RecordReader", e);
        }
    }

    private static class BatchScannerRecordReader extends RecordReader<Element, NullWritable> {

        private BatchScanner scanner;
        private Iterator<Map.Entry<Key, Value>> scannerIterator;
        private Element currentK;
        private MultiRangeInputSplit inputSplit;
        private AccumuloElementConverter converter;
        private View view;

        BatchScannerRecordReader(final String keyPackageClass, final Schema schema, final View view) throws StoreException,
                SchemaException, SerialisationException, UnsupportedEncodingException {
            super();
            final AccumuloKeyPackage keyPackage;
            try {
                keyPackage = Class.forName(keyPackageClass).asSubclass(AccumuloKeyPackage.class).newInstance();
            } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new StoreException("Unable to construct an instance of key package: " + keyPackageClass);
            }
            keyPackage.setSchema(schema);
            this.converter = keyPackage.getKeyConverter();
            this.view = view;
        }

        @Override
        public void initialize(final InputSplit split, final TaskAttemptContext context)
                throws IOException, InterruptedException {
            if (split instanceof MultiRangeInputSplit) {
                inputSplit = (MultiRangeInputSplit) split;
            }
            Connector connector = inputSplit.connector();
            try {
                Authorizations auths = InputConfigurator.getScanAuthorizations(AccumuloInputFormat.class,
                        context.getConfiguration());
                log.info("Initialising BatchScanner on table " + inputSplit.table() + " with auths " + auths);
                scanner = connector.createBatchScanner(inputSplit.table(), auths, 1);
            } catch (final TableNotFoundException e) {
                throw new IOException("Exception whilst initializing batch scanner: " + e);
            }
            scanner.setRanges(JavaConverters.asJavaCollectionConverter(inputSplit.ranges()).asJavaCollection());
            final List<IteratorSetting> iteratorSettings = JavaConverters.asJavaListConverter(inputSplit.iterators())
                    .asJava();
            for (final IteratorSetting is : iteratorSettings) {
                log.debug("Adding scan iterator " + is);
                scanner.addScanIterator(is);
            }
            Collection<Pair<Text, Text>> fetchedColumns = JavaConverters
                    .asJavaCollectionConverter(inputSplit.fetchedColumns()).asJavaCollection();
            for (final Pair<Text, Text> pair : fetchedColumns) {
                if (pair.getSecond() != null) {
                    scanner.fetchColumn(pair.getFirst(), pair.getSecond());
                } else {
                    scanner.fetchColumnFamily(pair.getFirst());
                }
            }
            scannerIterator = scanner.iterator();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (scannerIterator.hasNext()) {
                final Map.Entry<Key, Value> entry = scannerIterator.next();
                try {
                    currentK = converter.getFullElement(entry.getKey(), entry.getValue());
                    final ViewElementDefinition viewDef = view.getElement(currentK.getGroup());
                    if (viewDef != null) {
                        final ElementTransformer transformer = viewDef.getTransformer();
                        if (transformer != null) {
                            transformer.transform(currentK);
                        }
                    }
                } catch (AccumuloElementConversionException e) {
                    throw new IOException("Exception converting key-value pair to Element", e);
                }
                return true;
            }
            return false;
        }

        @Override
        public Element getCurrentKey() throws IOException, InterruptedException {
            return currentK;
        }

        @Override
        public NullWritable getCurrentValue() throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0.0F;
        }

        @Override
        public void close() throws IOException {
            scanner.close();
        }
    }
}
