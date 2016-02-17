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

package gaffer.operation.impl.get;

import gaffer.data.element.Edge;
import gaffer.operation.data.ElementSeed;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.GetOperation;

/**
 * Restricts {@link gaffer.operation.impl.get.GetElements} to only return {@link gaffer.data.element.Edge}s.
 * See implementations of {@link GetEdges} for further details.
 *
 * @param <SEED_TYPE> the seed seed type
 * @see gaffer.operation.impl.get.GetElements
 */
public abstract class GetEdges<SEED_TYPE extends ElementSeed> extends GetElements<SEED_TYPE, Edge> {
    public GetEdges() {
        super();
        setIncludeEdges(IncludeEdgeType.ALL);
    }

    public GetEdges(final Iterable<SEED_TYPE> seeds) {
        super(seeds);
        setIncludeEdges(IncludeEdgeType.ALL);
    }

    public GetEdges(final View view) {
        super(view);
        setIncludeEdges(IncludeEdgeType.ALL);
    }

    public GetEdges(final View view, final Iterable<SEED_TYPE> seeds) {
        super(view, seeds);
        setIncludeEdges(IncludeEdgeType.ALL);
    }

    public GetEdges(final GetOperation<SEED_TYPE, ?> operation) {
        super(operation);
    }

    @Override
    public boolean isIncludeEntities() {
        return false;
    }

    @Override
    public void setIncludeEntities(final boolean includeEntities) {
        if (includeEntities) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " does not support including entities");
        }
    }

    @Override
    public void setIncludeEdges(final IncludeEdgeType includeEdges) {
        if (IncludeEdgeType.NONE == includeEdges) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " requires edges to be included");
        }

        super.setIncludeEdges(includeEdges);
    }

    public static class Builder<OP_TYPE extends GetEdges<SEED_TYPE>, SEED_TYPE extends ElementSeed>
            extends GetElements.Builder<OP_TYPE, SEED_TYPE, Edge> {
        protected Builder(final OP_TYPE op) {
            super(op);
        }
    }
}
