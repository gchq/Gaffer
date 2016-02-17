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

package gaffer.accumulostore.operation.impl;

import gaffer.accumulostore.utils.Pair;
import gaffer.data.element.Edge;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.GetOperation;
import gaffer.operation.data.ElementSeed;

/**
 * Returns all {@link gaffer.data.element.Edge}'s between the provided
 * {@link gaffer.operation.data.ElementSeed}s.
 *
 */
public class GetEdgesInRanges<SEED_TYPE extends ElementSeed> extends GetElementsInRanges<SEED_TYPE, Edge> {

    public GetEdgesInRanges(final Iterable<Pair<SEED_TYPE>> seeds) {
        super(seeds);
        super.setIncludeEdges(IncludeEdgeType.ALL);
    }

    public GetEdgesInRanges(final View view) {
        super(view);
        super.setIncludeEdges(IncludeEdgeType.ALL);
    }

    public GetEdgesInRanges(final View view, final Iterable<Pair<SEED_TYPE>> seeds) {
        super(view, seeds);
        super.setIncludeEdges(IncludeEdgeType.ALL);
    }

    public GetEdgesInRanges(final GetOperation<Pair<SEED_TYPE>, Edge> operation) {
        super(operation);
        super.setIncludeEdges(IncludeEdgeType.ALL);
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

    public static class Builder<OP_TYPE extends GetEdgesInRanges<SEED_TYPE>, SEED_TYPE extends ElementSeed>
            extends GetElementsInRanges.Builder<OP_TYPE, SEED_TYPE, Edge> {

        protected Builder(final OP_TYPE op) {
            super(op);
        }

    }

}
