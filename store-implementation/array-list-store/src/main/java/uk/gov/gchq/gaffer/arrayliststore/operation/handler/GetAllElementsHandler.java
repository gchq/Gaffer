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

package uk.gov.gchq.gaffer.arrayliststore.operation.handler;

import uk.gov.gchq.gaffer.arrayliststore.ArrayListStore;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import java.util.ArrayList;
import java.util.List;

import static uk.gov.gchq.gaffer.operation.GetOperation.IncludeEdgeType;

public class GetAllElementsHandler implements OperationHandler<GetAllElements<Element>, CloseableIterable<Element>> {
    @Override
    public CloseableIterable<Element> doOperation(final GetAllElements<Element> operation,
                                         final Context context, final Store store) {
        return new WrappedCloseableIterable<>(doOperation(operation, (ArrayListStore) store));
    }

    private List<Element> doOperation(final GetAllElements<Element> operation,
                                      final ArrayListStore store) {
        final List<Element> result = new ArrayList<>();
        if (operation.isIncludeEntities()) {
            for (final Entity entity : store.getEntities()) {
                if (operation.validateFlags(entity) && operation.validate(entity)) {
                    result.add(entity);
                }
            }
        }
        if (!IncludeEdgeType.NONE.equals(operation.getIncludeEdges())) {
            for (final Edge edge : store.getEdges()) {
                if (operation.validateFlags(edge) && operation.validate(edge)) {
                    result.add(edge);
                }
            }
        }

        return result;
    }
}

