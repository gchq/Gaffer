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

package gaffer.arrayliststore;

import gaffer.arrayliststore.operation.handler.AddElementsHandler;
import gaffer.arrayliststore.operation.handler.GetAdjacentEntitySeedsHandler;
import gaffer.arrayliststore.operation.handler.GetElementsHandler;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.operation.Operation;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.operation.impl.get.GetElements;
import gaffer.store.Store;
import gaffer.store.StoreTrait;
import gaffer.store.operation.handler.OperationHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static gaffer.store.StoreTrait.FILTERING;
import static gaffer.store.StoreTrait.VALIDATION;


/**
 * A simple in memory {@link java.util.ArrayList} implementation of {@link Store}.
 * <p>
 * This store holds 2 {@link java.util.ArrayList}s one for {@link Entity} and one for
 * {@link Edge}. As the elements are simply
 * stored in lists they are not serialised and not indexed, so look ups require full scans.
 */
public class ArrayListStore extends Store {
    private static final List<StoreTrait> TRAITS = Arrays.asList(FILTERING, VALIDATION);
    private final List<Entity> entities = new ArrayList<>();
    private final List<Edge> edges = new ArrayList<>();

    @Override
    protected Collection<StoreTrait> getTraits() {
        return TRAITS;
    }

    @Override
    protected boolean isValidationRequired() {
        return false;
    }

    @Override
    protected OperationHandler<GetElements<ElementSeed, Element>, Iterable<Element>> getGetElementsHandler() {
        return new GetElementsHandler();
    }

    @Override
    protected OperationHandler<? extends GetAdjacentEntitySeeds, Iterable<EntitySeed>> getAdjacentEntitySeedsHandler() {
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
        // no additional operations supported
    }

    @Override
    protected <OUTPUT> OUTPUT doUnhandledOperation(final Operation<?, OUTPUT> operation) {
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
