/*
 * Copyright 2020 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.scalardd;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.apache.spark.rdd.RDD;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;
import scala.collection.TraversableOnce;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction1;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.operation.scalardd.SplitStoreFromRDDOfElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractSplitStoreFromRDDOfElementsHandler;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.Serializable;
import java.util.List;
import java.util.Random;

import static scala.reflect.ClassTag$.MODULE$;

public class SplitStoreFromRDDOfElementsHandler extends AbstractSplitStoreFromRDDOfElementsHandler<SplitStoreFromRDDOfElements> {

    private static final ClassTag<Pair<Key, Key>> PAIR_CLASS_TAG = MODULE$.apply(Pair.class);
    private static final ClassTag<String> STRING_CLASS_TAG = MODULE$.apply(String.class);
    private static final ClassTag<Text> TEXT_CLASS_TAG = MODULE$.apply(Text.class);
    private static final boolean PRESERVE_PARTITIONING = true;
    private static final boolean WITHOUT_REPLACEMENT = false;

    @Override
    public Void doOperation(final SplitStoreFromRDDOfElements operation, final Context context, final Store store) throws OperationException {

        generateSplitPoints(operation, context, (AccumuloStore) store);
        return null;
    }

    private void generateSplitPoints(final SplitStoreFromRDDOfElements operation, final Context context, final AccumuloStore store) throws OperationException {

        final byte[] schemaAsJson = store.getSchema().toCompactJson();
        final String keyConverterClassName = store.getKeyPackage().getKeyConverter().getClass().getName();

        final RDD<Text> rows = operation.getInput()
                .mapPartitions(new ElementIteratorToPairIteratorFunction(keyConverterClassName, schemaAsJson), PRESERVE_PARTITIONING, PAIR_CLASS_TAG)
                .flatMap(new KeyPairToRowFunction(), TEXT_CLASS_TAG);

        final double fractionToSample = super.adjustFractionToSampleForSize(
                operation.getFractionToSample(),
                operation.getMaxSampleSize(),
                rows.count());

        final Random seed = new Random(System.currentTimeMillis());

        final List<String> sample = rows.sample(WITHOUT_REPLACEMENT, fractionToSample, seed.nextLong())
                .map(new TextToStringFunction(), STRING_CLASS_TAG)
                .toJavaRDD()
                .collect();

        super.createSplitPoints(store, context, sample);
    }

    private static class ElementIteratorToPairIteratorFunction extends AbstractFunction1<Iterator<Element>, Iterator<Pair<Key, Key>>> implements Serializable {

        private final String keyConverterClassName;
        private final byte[] schemaAsJson;

        ElementIteratorToPairIteratorFunction(final String keyConverterClassName, final byte[] schemaAsJson) {
            this.keyConverterClassName = keyConverterClassName;
            this.schemaAsJson = schemaAsJson;
        }

        @Override
        public Iterator<Pair<Key, Key>> apply(final Iterator<Element> elementIterator) {

            try {

                final AccumuloElementConverter converter = Class.forName(keyConverterClassName).asSubclass(AccumuloElementConverter.class)
                        .getConstructor(Schema.class)
                        .newInstance(Schema.fromJson(schemaAsJson));

                return new AbstractIterator<Pair<Key, Key>>() {

                    @Override
                    public boolean hasNext() {
                        return elementIterator.hasNext();
                    }

                    @Override
                    public Pair<Key, Key> next() {
                        return converter.getKeysFromElement(elementIterator.next());
                    }
                };

            } catch (final Exception exception) {

                throw new RuntimeException(exception);
            }
        }
    }

    private static class KeyPairToRowFunction extends AbstractFunction1<Pair<Key, Key>, TraversableOnce<Text>> implements Serializable {

        @Override
        public TraversableOnce<Text> apply(final Pair<Key, Key> pair) {

            final ArrayBuffer<Text> buf = new ArrayBuffer<>();

            final Key first = pair.getFirst();
            if (null != first) {
                buf.$plus$eq(new Text(first.getRow()));
            }
            final Key second = pair.getSecond();
            if (null != second) {
                buf.$plus$eq(new Text(second.getRow()));
            }

            return buf;
        }
    }

    private static class TextToStringFunction extends AbstractFunction1<Text, String> implements Serializable {

        @Override
        public String apply(final Text text) {
            return text.toString();
        }
    }

}
