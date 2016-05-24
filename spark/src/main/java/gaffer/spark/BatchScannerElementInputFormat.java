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

package gaffer.spark;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import gaffer.accumulostore.key.AccumuloElementConverter;
import gaffer.accumulostore.key.AccumuloKeyPackage;
import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.data.element.Element;
import gaffer.data.element.Properties;
import gaffer.data.elementdefinition.exception.SchemaException;
import gaffer.exception.SerialisationException;
import gaffer.serialisation.simple.StringSerialiser;
import gaffer.store.StoreException;
import gaffer.store.schema.Schema;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An {@link InputFormatBase} that allows a MapReduce job to consume data from the
 * Accumulo table underlying Gaffer. This uses
 * {@link BatchScannerElementInputFormat} to provide the splits and uses a
 * {@link BatchScanner} within the {@link RecordReader} to provide the keys and
 * values.
 */
public class BatchScannerElementInputFormat extends InputFormatBase<Element, Properties> {

    public static final String KEY_PACKAGE = "KEY_PACKAGE";
    public static final String SCHEMA = "SCHEMA";

    @Override
    public List<InputSplit> getSplits(final JobContext context) throws IOException {
        return new BatchAccumuloInputFormat().getSplits(context);
    }

    @Override
    public RecordReader<Element, Properties> createRecordReader(final InputSplit split,
            final TaskAttemptContext context) throws IOException, InterruptedException {
        log.setLevel(getLogLevel(context));
        String keyPackageClass = context.getConfiguration().get(KEY_PACKAGE);
        String schema = context.getConfiguration().get(SCHEMA);
        try {
            return new BatchScannerRecordReader(keyPackageClass, schema);
        } catch (StoreException | SchemaException | SerialisationException e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    static class BatchScannerRecordReader extends RecordReader<Element, Properties> {

        private BatchScanner scanner;
        private Iterator<Map.Entry<Key, Value>> scannerIterator;
        private Element currentK;
        private Properties currentV;
        private MultiRangeInputSplit inputSplit;
        private AccumuloElementConverter converter;
        private StringSerialiser serialiser = new StringSerialiser();

        BatchScannerRecordReader(final String keyPackageClass, final String schema) throws StoreException, SchemaException, SerialisationException {
            super();
            AccumuloKeyPackage keyPackage;
            try {
                keyPackage = Class.forName(keyPackageClass).asSubclass(AccumuloKeyPackage.class).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new StoreException("Unable to construct an instance of key package: " + keyPackageClass);
            }
            keyPackage.setSchema(Schema.fromJson(serialiser.serialise(schema)));
            converter = keyPackage.getKeyConverter();
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
            } catch (TableNotFoundException e) {
                throw new IOException("Exception whilst initializing batch scanner: " + e);
            }
            scanner.setRanges(JavaConverters.asJavaCollectionConverter(inputSplit.ranges()).asJavaCollection());
            List<IteratorSetting> iteratorSettings = JavaConverters.asJavaListConverter(inputSplit.iterators())
                    .asJava();
            for (IteratorSetting is : iteratorSettings) {
                log.debug("Adding scan iterator " + is);
                scanner.addScanIterator(is);
            }
            Collection<Pair<Text, Text>> fetchedColumns = JavaConverters
                    .asJavaCollectionConverter(inputSplit.fetchedColumns()).asJavaCollection();
            for (Pair<Text, Text> pair : fetchedColumns) {
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
                Map.Entry<Key, Value> entry = scannerIterator.next();
                try {
                    currentK = converter.getFullElement(entry.getKey(), entry.getValue());
                    currentV = currentK.getProperties();
                } catch (AccumuloElementConversionException e) {
                    e.printStackTrace();
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
        public Properties getCurrentValue() throws IOException, InterruptedException {
            return currentV;
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
