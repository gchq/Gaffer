/*
 * Copyright 2017 Crown Copyright
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
import org.apache.accumulo.core.data.Value;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.operation.scalardd.ImportRDDOfElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.scalardd.ImportKeyValuePairRDDToAccumulo;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.utils.scala.ElementConverterFunction;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

public class ImportRDDOfElementsHandler implements OperationHandler<ImportRDDOfElements, Void> {
    private static final String OUTPUT_PATH = "outputPath";
    private static final String FAILURE_PATH = "failurePath";
    private static final ClassTag<Tuple2<Key, Value>> TUPLE2_CLASS_TAG = scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class);
    private static final ClassTag<AccumuloElementConverter> ACCUMULO_ELEMENT_CONVERTER_CLASS_TAG = scala.reflect.ClassTag$.MODULE$.apply(AccumuloElementConverter.class);

    @Override
    public Void doOperation(final ImportRDDOfElements operation, final Context context, final Store store) throws OperationException {
        doOperation(operation, context, (AccumuloStore) store);
        return null;
    }

    public void doOperation(final ImportRDDOfElements operation, final Context context, final AccumuloStore store) throws OperationException {
        final String outputPath = operation.getOption(OUTPUT_PATH);
        if (null == outputPath || outputPath.isEmpty()) {
            throw new OperationException("Option outputPath must be set for this option to be run against the accumulostore");
        }
        final String failurePath = operation.getOption(FAILURE_PATH);
        if (null == failurePath || failurePath.isEmpty()) {
            throw new OperationException("Option failurePath must be set for this option to be run against the accumulostore");
        }
        final ElementConverterFunction func = new ElementConverterFunction(operation.getSparkContext().broadcast(store.getKeyPackage().getKeyConverter(), ACCUMULO_ELEMENT_CONVERTER_CLASS_TAG));
        final RDD<Tuple2<Key, Value>> rdd = operation.getInput().flatMap(func, TUPLE2_CLASS_TAG);
        final ImportKeyValuePairRDDToAccumulo op =
                new ImportKeyValuePairRDDToAccumulo.Builder()
                        .input(rdd)
                        .failurePath(failurePath)
                        .outputPath(outputPath)
                        .build();
        store._execute(new OperationChain<>(op), context);
    }
}

