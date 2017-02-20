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

package uk.gov.gchq.gaffer.arrayliststore;

import uk.gov.gchq.gaffer.arrayliststore.operation.handler.AddElementsHandler;
import uk.gov.gchq.gaffer.arrayliststore.operation.handler.GetAdjacentEntitySeedsHandler;
import uk.gov.gchq.gaffer.arrayliststore.operation.handler.GetAllElementsHandler;
import uk.gov.gchq.gaffer.arrayliststore.operation.handler.GetElementsHandler;
import uk.gov.gchq.gaffer.arrayliststore.operation.handler.InitialiseArrayListStoreExport;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.export.InitialiseExportHandler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;


/**
 * A simple in memory {@link java.util.ArrayList} implementation of {@link Store}.
 * <p>
 * This store holds 2 {@link java.util.ArrayList}s one for {@link Entity} and one for
 * {@link Edge}. As the elements are simply
 * stored in lists they are not serialised and not indexed, so look ups require full scans.
 */
public class ArrayListStore extends Store {
    private static final Set<StoreTrait> TRAITS = new HashSet<>(Collections.singletonList(PRE_AGGREGATION_FILTERING));
    private final List<Entity> entities = new ArrayList<>();
    private final List<Edge> edges = new ArrayList<>();

    @Override
    public Set<StoreTrait> getTraits() {
        return TRAITS;
    }

    @Override
    public boolean isValidationRequired() {
        return false;
    }

    @Override
    protected OperationHandler<GetElements<ElementSeed, Element>, CloseableIterable<Element>> getGetElementsHandler() {
        return new GetElementsHandler();
    }

    @Override
    protected OperationHandler<GetAllElements<Element>, CloseableIterable<Element>> getGetAllElementsHandler() {
        return new GetAllElementsHandler();
    }

    @Override
    protected OperationHandler<? extends GetAdjacentEntitySeeds, CloseableIterable<EntitySeed>> getAdjacentEntitySeedsHandler() {
        return new GetAdjacentEntitySeedsHandler();
    }

    @Override
    protected OperationHandler<? extends AddElements, Void> getAddElementsHandler() {
        return new AddElementsHandler();
    }

    /**
     * This store does not support any other optional operations.
     */
    @Override
    protected void addAdditionalOperationHandlers() {
        addOperationHandler(InitialiseArrayListStoreExport.class, new InitialiseExportHandler());
    }

    @Override
    protected <OUTPUT> OUTPUT doUnhandledOperation(final Operation<?, OUTPUT> operation, final Context context) {
        throw new UnsupportedOperationException("I do not know how to handle: " + operation.getClass().getSimpleName());
    }

    public List<Entity> getEntities() {
        return entities;
    }

    public List<Edge> getEdges() {
        return edges;
    }

    public void addElements(final Iterable<Element> elements) {
        for (final Element element : elements) {
            if (element instanceof Entity) {
                entities.add((Entity) element);
            } else {
                // Assume it is an Edge
                edges.add((Edge) element);
            }
        }
    }
}
