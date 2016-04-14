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

import gaffer.arrayliststore.ArrayListStore;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.simple.MigrateElements;
import gaffer.store.Store;
import gaffer.store.operation.handler.OperationHandler;
import java.util.ArrayList;
import java.util.List;

public class MigrateElementsHandler implements OperationHandler<MigrateElements, Void> {
    @Override
    public Void doOperation(final MigrateElements operation, final Store store) {
        doOperation(operation, (ArrayListStore) store);
        return null;
    }

    private List<Element> doOperation(final MigrateElements operation, final ArrayListStore store) {
        final boolean hasSeeds = operation.getSeeds().iterator().hasNext();
        final List<Element> result = new ArrayList<>();
        final View view = operation.getView();
        if (!operation.getView().getEntityGroups().isEmpty()) {
            for (final Entity entity : store.getEntities()) {
                if ((!hasSeeds || isSeedEqual(ElementSeed.createSeed(entity), operation.getSeeds()))
                        && operation.validateFilter(entity)) {
                    final ViewElementDefinition elementDef = view.getEntity(entity.getGroup());
                    if (null != elementDef && null != elementDef.getTransformer()) {
                        elementDef.getTransformer().transform(entity);
                    }
                }
            }
        }

        if (!operation.getView().getEdgeGroups().isEmpty()) {
            for (final Edge edge : store.getEdges()) {
                if ((!hasSeeds || isSeedEqual(ElementSeed.createSeed(edge), operation.getSeeds()))
                        && operation.validateFilter(edge)) {
                    final ViewElementDefinition elementDef = view.getEdge(edge.getGroup());
                    if (null != elementDef && null != elementDef.getTransformer()) {
                        elementDef.getTransformer().transform(edge);
                    }
                }
            }
        }

        return result;
    }

    private boolean isSeedEqual(final ElementSeed elementSeed, final Iterable<ElementSeed> seeds) {
        for (final ElementSeed seed : seeds) {
            if (elementSeed.equals(seed)) {
                return true;
            }
        }

        return false;
    }
}

