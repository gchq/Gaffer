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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.javardd;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.operation.javardd.ImportJavaRDDOfElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.javardd.ImportKeyValueJavaPairRDDToAccumulo;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ImportJavaRDDOfElementsHandler implements OperationHandler<ImportJavaRDDOfElements, Void> {

    private static final String OUTPUT_PATH = "outputPath";

    @Override
    public Void doOperation(final ImportJavaRDDOfElements operation, final Context context, final Store store) throws OperationException {
        doOperation(operation, context, (AccumuloStore) store);
        return null;
    }

    public void doOperation(final ImportJavaRDDOfElements operation, final Context context, final AccumuloStore store) throws OperationException {
        ElementConverterPairFlatMapFunction func = new ElementConverterPairFlatMapFunction(store.getKeyPackage().getKeyConverter());
        JavaPairRDD<Key, Value> rdd = operation.getInput().flatMapToPair(func);
        ImportKeyValueJavaPairRDDToAccumulo op = new ImportKeyValueJavaPairRDDToAccumulo.Builder().input(rdd).outputPath(OUTPUT_PATH).build();
        store.execute(op, context.getUser());
    }

    private class ElementConverterPairFlatMapFunction implements PairFlatMapFunction<Element, Key, Value>, Serializable {

        protected ElementConverterPairFlatMapFunction(final AccumuloElementConverter converter) {
            this.converter = converter;
        }

        private List<Tuple2<Key, Value>> tuples = new ArrayList<>(2);
        AccumuloElementConverter converter;

        @Override
        public Iterator<Tuple2<Key, Value>> call(final Element e) throws Exception {
            Pair<Key> keys = converter.getKeysFromElement(e);
            Value value = converter.getValueFromElement(e);
            tuples.add(new Tuple2<>(keys.getFirst(), value));
            Key second = keys.getSecond();
            if (second != null) {
                tuples.add(new Tuple2<>(second, value));
            }
            Iterator iter = tuples.listIterator();
            tuples.clear();
            return iter;
        }
    };

}


