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

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.spark.api.java.JavaPairRDD;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractImportKeyValuePairRDDToAccumuloHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.javardd.ImportKeyValueJavaPairRDDToAccumulo;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.utils.AccumuloKeyRangePartitioner;

public class ImportKeyValueJavaPairRDDToAccumuloHandler extends AbstractImportKeyValuePairRDDToAccumuloHandler<ImportKeyValueJavaPairRDDToAccumulo> {
    @Override
    protected void prepareKeyValues(final ImportKeyValueJavaPairRDDToAccumulo operation, final AccumuloKeyRangePartitioner partitioner) throws OperationException {
        final JavaPairRDD<Key, Value> rdd = operation.getInput().repartitionAndSortWithinPartitions(partitioner);
        rdd.saveAsNewAPIHadoopFile(operation.getOutputPath(), Key.class, Value.class, AccumuloFileOutputFormat.class, getConfiguration(operation));
    }

    @Override
    protected String getFailurePath(final ImportKeyValueJavaPairRDDToAccumulo operation) {
        return operation.getFailurePath();
    }

    @Override
    protected String getOutputPath(final ImportKeyValueJavaPairRDDToAccumulo operation) {
        return operation.getOutputPath();
    }
}
