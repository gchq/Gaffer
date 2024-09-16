/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.inputformat;

import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloKeyPackage;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map.Entry;

/**
 * An {@link InputFormatBase} that allows the data in an Accumulo store to be read as {@link Element},
 * {@link NullWritable} pairs.
 */
public class ElementInputFormat extends InputFormatBase<Element, NullWritable> {

    public static final String KEY_PACKAGE = "KEY_PACKAGE";
    public static final String SCHEMA = AccumuloStoreConstants.SCHEMA;
    public static final String VIEW = AccumuloStoreConstants.VIEW;

    @Override
    public RecordReader<Element, NullWritable> createRecordReader(final InputSplit split, final TaskAttemptContext context)
            throws IOException {
        log.setLevel(getLogLevel(context));
        final Configuration conf = context.getConfiguration();
        final String keyPackageClass = conf.get(KEY_PACKAGE);
        final Schema schema = Schema.fromJson(conf.get(SCHEMA).getBytes(StandardCharsets.UTF_8));
        final View view = View.fromJson(conf.get(VIEW).getBytes(StandardCharsets.UTF_8));
        try {
            return new ElementWithPropertiesRecordReader(keyPackageClass, schema, view);
        } catch (final StoreException | SchemaException | SerialisationException e) {
            throw new IOException("Exception creating RecordReader", e);
        }
    }

    private static class ElementWithPropertiesRecordReader extends InputFormatBase.RecordReaderBase<Element, NullWritable> {

        private final AccumuloElementConverter converter;
        private final View view;

        ElementWithPropertiesRecordReader(final String keyPackageClass, final Schema schema, final View view)
                throws StoreException, SchemaException, SerialisationException {
            super();
            final AccumuloKeyPackage keyPackage;
            try {
                keyPackage = Class.forName(keyPackageClass).asSubclass(AccumuloKeyPackage.class).newInstance();
            } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new StoreException("Unable to construct an instance of key package: " + keyPackageClass, e);
            }
            keyPackage.setSchema(schema);
            this.converter = keyPackage.getKeyConverter();
            this.view = view;
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            currentV = NullWritable.get();
            while (scannerIterator.hasNext()) {
                ++numKeysRead;
                final Entry<Key, Value> entry = scannerIterator.next();
                try {
                    currentK = converter.getFullElement(entry.getKey(), entry.getValue(), false);
                    final ViewElementDefinition viewDef = view.getElement(currentK.getGroup());
                    if (null != viewDef) {
                        final ElementTransformer transformer = viewDef.getTransformer();
                        if (null != transformer) {
                            transformer.apply(currentK);
                        }
                    }
                    if (doPostFilter(currentK, view)) {
                        ViewUtil.removeProperties(view, currentK);
                        return true;
                    }
                } catch (final AccumuloElementConversionException e) {
                    throw new IOException("Exception converting the key-value to an Element:", e);
                }
            }
            return false;
        }
    }

    public static boolean doPostFilter(final Element element, final View view) {
        final ViewElementDefinition viewDef = view.getElement(element.getGroup());
        if (null != viewDef) {
            return postFilter(element, viewDef.getPostTransformFilter());
        }
        return true;
    }

    public static boolean postFilter(final Element element, final ElementFilter postFilter) {
        return null == postFilter || postFilter.test(element);
    }

}
