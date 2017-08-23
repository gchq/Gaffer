/*
 * Copyright 2016-2017 Crown Copyright
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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.operation.javardd.GetJavaRDDOfAllElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfAllElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.scalardd.GetRDDOfAllElementsHandler;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

public class GetJavaRDDOfAllElementsHandler extends AbstractGetRDDHandler<GetJavaRDDOfAllElements, JavaRDD<Element>> {

    @Override
    public JavaRDD<Element> doOperation(final GetJavaRDDOfAllElements operation,
                                        final Context context,
                                        final Store store) throws OperationException {
        return doOperation(operation, context, (AccumuloStore) store);
    }

    private JavaRDD<Element> doOperation(final GetJavaRDDOfAllElements operation,
                                         final Context context,
                                         final AccumuloStore accumuloStore) throws OperationException {
        final SparkSession sparkSession = SparkSession.builder()
                .master(operation.getJavaSparkContext().master())
                .appName(operation.getJavaSparkContext().appName())
                .config(operation.getJavaSparkContext().getConf())
                .getOrCreate();
        final GetRDDOfAllElements getRDDOfAllElements = new GetRDDOfAllElements.Builder()
                .sparkSession(sparkSession)
                .directedType(operation.getDirectedType())
                .view(operation.getView())
                .options(operation.getOptions())
                .build();
        final RDD<Element> scalaRDD = new GetRDDOfAllElementsHandler().doOperation(getRDDOfAllElements, context, accumuloStore);
        return scalaRDD.toJavaRDD();
    }
}
