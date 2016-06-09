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

package gaffer.arrayliststore.operation.handler;

import static gaffer.operation.GetOperation.IncludeEdgeType;

import gaffer.arrayliststore.ArrayListStore;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.operation.impl.get.GetAllElements;
import gaffer.store.Context;
import gaffer.store.Store;
import gaffer.store.operation.handler.OperationHandler;
import java.util.ArrayList;
import java.util.List;

public class GetAllElementsHandler implements OperationHandler<GetAllElements<Element>, Iterable<Element>> {
    @Override
    public Iterable<Element> doOperation(final GetAllElements<Element> operation,
                                         final Context context, final Store store) {
        return doOperation(operation, (ArrayListStore) store);
    }

    private List<Element> doOperation(final GetAllElements<Element> operation,
                                      final ArrayListStore store) {
        final List<Element> result = new ArrayList<>();
        if (operation.isIncludeEntities()) {
            for (final Entity entity : store.getEntities()) {
                if (operation.validateFlags(entity) && operation.validateFilter(entity)) {
                    result.add(entity);
                }
            }
        }
        if (!IncludeEdgeType.NONE.equals(operation.getIncludeEdges())) {
            for (final Edge edge : store.getEdges()) {
                if (operation.validateFlags(edge) && operation.validateFilter(edge)) {
                    result.add(edge);
                }
            }
        }

        return result;
    }
}

