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

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.spark.rdd.OrderedRDDFunctions;
import org.apache.spark.rdd.PairRDDFunctions;
import scala.Tuple2;
import scala.math.Ordering;
import scala.math.Ordering$;
import scala.reflect.ClassTag;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractImportKeyValuePairRDDToAccumuloHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.scalardd.ImportKeyValuePairRDDToAccumulo;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.utils.AccumuloKeyRangePartitioner;
import java.util.Comparator;

public class ImportKeyValuePairRDDToAccumuloHandler extends AbstractImportKeyValuePairRDDToAccumuloHandler<ImportKeyValuePairRDDToAccumulo> {
    private static final ClassTag<Key> KEY_CLASS_TAG = scala.reflect.ClassTag$.MODULE$.apply(Key.class);
    private static final ClassTag<Value> VALUE_CLASS_TAG = scala.reflect.ClassTag$.MODULE$.apply(Value.class);
    private static final Ordering<Key> ORDERING_CLASS_TAG = Ordering$.MODULE$.comparatorToOrdering(Comparator.<Key>naturalOrder());

    @Override
    protected void prepareKeyValues(final ImportKeyValuePairRDDToAccumulo operation, final AccumuloKeyRangePartitioner partitioner) throws OperationException {
        final OrderedRDDFunctions orderedRDDFunctions = new OrderedRDDFunctions(operation.getInput(), ORDERING_CLASS_TAG, KEY_CLASS_TAG, VALUE_CLASS_TAG, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class));
        final PairRDDFunctions pairRDDFunctions = new PairRDDFunctions(orderedRDDFunctions.repartitionAndSortWithinPartitions(partitioner), KEY_CLASS_TAG, VALUE_CLASS_TAG, ORDERING_CLASS_TAG);
        pairRDDFunctions.saveAsNewAPIHadoopFile(operation.getOutputPath(), Key.class, Value.class, AccumuloFileOutputFormat.class, getConfiguration(operation));
    }

    @Override
    protected String getFailurePath(final ImportKeyValuePairRDDToAccumulo operation) {
        return operation.getFailurePath();
    }

    @Override
    protected String getOutputPath(final ImportKeyValuePairRDDToAccumulo operation) {
        return operation.getOutputPath();
    }
}
