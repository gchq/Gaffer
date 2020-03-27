package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.javardd;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.operation.javardd.SplitStoreFromJavaRDDOfElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractSplitStoreFromRDDOfElementsHandler;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static java.util.Arrays.asList;

public class SplitStoreFromJavaRDDOfElementsHandler extends AbstractSplitStoreFromRDDOfElementsHandler<SplitStoreFromJavaRDDOfElements> {

    private static final boolean WITHOUT_REPLACEMENT = false;

    @Override
    public Void doOperation(final SplitStoreFromJavaRDDOfElements operation, final Context context, final Store store) throws OperationException {

        generateSplitPoints(operation, context, (AccumuloStore) store);
        return null;
    }

    private void generateSplitPoints(final SplitStoreFromJavaRDDOfElements operation, final Context context, final AccumuloStore store) throws OperationException {

        final byte[] schemaAsJson = store.getSchema().toCompactJson();
        final String keyConverterClassName = store.getKeyPackage().getKeyConverter().getClass().getName();

        final JavaRDD<Text> rows = operation.getInput().mapPartitions(new ElementIteratorToPairIteratorFunction(keyConverterClassName, schemaAsJson))
                .flatMap(pair -> {
                    if (null == pair.getSecond()) {
                        return asList(pair.getFirst()).iterator();
                    } else {
                        return asList(pair.getFirst(), pair.getSecond()).iterator();
                    }
                })
                .map(key -> key.getRow());

        final double fractionToSample = super.adjustFractionToSampleForSize(
                operation.getFractionToSample(),
                operation.getMaxSampleSize(),
                rows.count());

        final Random seed = new Random(System.currentTimeMillis());

        final List<String> sample = rows.sample(WITHOUT_REPLACEMENT, fractionToSample, seed.nextLong())
                .map(Text::toString)
                .collect();

        super.createSplitPoints(store, context, sample);
    }

    private static class ElementIteratorToPairIteratorFunction implements FlatMapFunction<Iterator<Element>, Pair<Key, Key>> {

        private final String keyConverterClassName;
        private final byte[] schemaAsJson;

        public ElementIteratorToPairIteratorFunction(final String keyConverterClassName, final byte[] schemaAsJson) {
            this.keyConverterClassName = keyConverterClassName;
            this.schemaAsJson = schemaAsJson;
        }

        @Override
        public Iterator<Pair<Key, Key>> call(final Iterator<Element> elementIterator) throws Exception {

            final AccumuloElementConverter converter = Class.forName(keyConverterClassName).asSubclass(AccumuloElementConverter.class)
                    .getConstructor(Schema.class)
                    .newInstance(Schema.fromJson(schemaAsJson));

            return new Iterator<Pair<Key, Key>>() {

                @Override
                public boolean hasNext() {
                    return elementIterator.hasNext();
                }

                public Pair<Key, Key> next() {
                    return converter.getKeysFromElement(elementIterator.next());
                }
            };
        }
    }
}
