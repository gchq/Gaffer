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

package uk.gov.gchq.gaffer.hbasestore.operation.handler;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetAllElementsHandler implements OperationHandler<GetAllElements<Element>, CloseableIterable<Element>> {
    @Override
    public CloseableIterable<Element> doOperation(final GetAllElements<Element> operation, final Context context, final Store store)
            throws OperationException {
        return doOperation(operation, context.getUser(), (HBaseStore) store);
    }

    public CloseableIterable<Element> doOperation(final GetAllElements<Element> operation, final User user, final HBaseStore store) throws OperationException {
        final ElementSerialisation serialisation = new ElementSerialisation(store.getSchema());

        // TODO: don't load all the results into an array list.
        final List<Element> elements = new ArrayList<>();
        try {
            final Table table = store.getConnection().getTable(store.getProperties().getTable());

            final Scan scan = new Scan();
            final ResultScanner scanner = table.getScanner(scan);
            for (Result result = scanner.next(); (result != null); result = scanner.next()) {
                for (final Cell cell : result.listCells()) {
                    elements.add(serialisation.getElement(cell));
                }
            }
        } catch (final IOException | StoreException e) {
            throw new OperationException("Failed to get elements", e);
        }

        return new WrappedCloseableIterable<>(elements);
    }
}
