/*
 * Copyright 2017. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.operation.addelements.handler.ImportRDDOfElements;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.spark.operation.javardd.ImportJavaRDDOfElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

/**
 * An {@link OperationHandler} for the {@link ImportJavaRDDOfElements} operation on the {@link ParquetStore}.
 */
public class ImportJavaRDDOfElementsHandler implements OperationHandler<ImportJavaRDDOfElements> {

    @Override
    public Void doOperation(final ImportJavaRDDOfElements operation, final Context context, final Store store)
            throws OperationException {
        new AddElementsFromRDD().addElementsFromRDD(operation.getInput(), context, (ParquetStore) store);
        return null;
    }

}
