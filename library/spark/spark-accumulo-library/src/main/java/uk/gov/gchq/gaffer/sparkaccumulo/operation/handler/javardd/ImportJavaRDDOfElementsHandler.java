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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.javardd;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.operation.javardd.ImportJavaRDDOfElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.javardd.ImportKeyValueJavaPairRDDToAccumulo;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.utils.java.ElementConverterFunction;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

public class ImportJavaRDDOfElementsHandler implements OperationHandler<ImportJavaRDDOfElements, Void> {

    private static final String OUTPUT_PATH = "outputPath";
    private static final String FAILURE_PATH = "failurePath";

    @Override
    public Void doOperation(final ImportJavaRDDOfElements operation, final Context context, final Store store) throws OperationException {
        doOperation(operation, context, (AccumuloStore) store);
        return null;
    }

    public void doOperation(final ImportJavaRDDOfElements operation, final Context context, final AccumuloStore store) throws OperationException {
        final String outputPath = operation.getOption(OUTPUT_PATH);
        if (null == outputPath || outputPath.isEmpty()) {
            throw new OperationException("Option outputPath must be set for this option to be run against the accumulostore");
        }
        final String failurePath = operation.getOption(FAILURE_PATH);
        if (null == failurePath || failurePath.isEmpty()) {
            throw new OperationException("Option failurePath must be set for this option to be run against the accumulostore");
        }

        final Broadcast<AccumuloElementConverter> broadcast = operation.getJavaSparkContext().broadcast(store.getKeyPackage().getKeyConverter());
        final ElementConverterFunction func = new ElementConverterFunction(broadcast);
        final JavaPairRDD<Key, Value> rdd = operation.getInput().flatMapToPair(func);
        final ImportKeyValueJavaPairRDDToAccumulo op = new ImportKeyValueJavaPairRDDToAccumulo.Builder().input(rdd).failurePath(failurePath).outputPath(outputPath).build();
        store._execute(new OperationChain<>(op), context);
    }
}


