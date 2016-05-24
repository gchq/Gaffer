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

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.hadoop.mapreduce.InputSplit;
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

/**
 * An {@link InputFormatBase} that allows a MapReduce job to consume data from the
 * Accumulo table underlying Gaffer.
 */
public class ElementInputFormat extends InputFormatBase<Element, Properties> {

    public static final String KEY_PACKAGE = "KEY_PACKAGE";
    public static final String SCHEMA = "SCHEMA";

    @Override
    public RecordReader<Element, Properties> createRecordReader(final InputSplit split, final TaskAttemptContext context)
            throws IOException, InterruptedException {
        log.setLevel(getLogLevel(context));
        String keyPackageClass = context.getConfiguration().get(KEY_PACKAGE);
        String schema = context.getConfiguration().get(SCHEMA);
        try {
            return new ElementWithPropertiesRecordReader(keyPackageClass, schema);
        } catch (StoreException | SchemaException | SerialisationException e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    class ElementWithPropertiesRecordReader extends InputFormatBase.RecordReaderBase<Element, Properties> {

        private AccumuloElementConverter converter;
        private StringSerialiser serialiser = new StringSerialiser();

        ElementWithPropertiesRecordReader(final String keyPackageClass, final String schema) throws StoreException, SchemaException, SerialisationException {
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
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (scannerIterator.hasNext()) {
                ++numKeysRead;
                Entry<Key, Value> entry = scannerIterator.next();

                try {
                    currentK = converter.getFullElement(entry.getKey(), entry.getValue());
                    currentV = currentK.getProperties();
                } catch (AccumuloElementConversionException e) {
                    e.printStackTrace();
                }
                if (log.isTraceEnabled()) {
                    log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
                }
                return true;
            }
            return false;
        }
    }
}
