/*
 * Copyright 2016-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.operation.javardd.GetJavaRDDOfAllElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfAllElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.scalardd.GetRDDOfAllElementsHandler;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

/**
 * A handler for the {@link GetJavaRDDOfAllElements} operation. This simply uses the operation
 * {@link GetRDDOfAllElements} to produce an {@link RDD} and then calls {@code toJavaRDD()} to obtain a
 * {@link JavaRDD}.
 * <p>
 * <p>If the {@code gaffer.accumulo.spark.directrdd.use_rfile_reader} option is set to {@code true} then the
 * RDD will be produced by directly reading the RFiles in the Accumulo table, rather than using
 * {@link uk.gov.gchq.gaffer.accumulostore.inputformat.ElementInputFormat} to get data via the tablet servers. In order
 * to read the RFiles directly, the user must have read access to the files. Also note that any data that has not been
 * minor compacted will not be read. Reading the Rfiles directly can increase the performance.
 * <p>
 * <p>If the {@code gaffer.accumulo.spark.directrdd.use_rfile_reader} option is not set then the standard approach
 * of obtaining data via the tablet servers is used.
 */
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
        final GetRDDOfAllElements getRDDOfAllElements = new GetRDDOfAllElements.Builder()
                .directedType(operation.getDirectedType())
                .view(operation.getView())
                .options(operation.getOptions())
                .build();
        final RDD<Element> scalaRDD = new GetRDDOfAllElementsHandler().doOperation(getRDDOfAllElements, context, accumuloStore);
        return scalaRDD.toJavaRDD();
    }
}
