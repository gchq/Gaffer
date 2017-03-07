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

package uk.gov.gchq.gaffer.accumulostore.operation.impl;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.AbstractGetIterableElementsOperation;
import uk.gov.gchq.gaffer.operation.GetIterableElementsOperation;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;

/**
 * Returns {@link uk.gov.gchq.gaffer.data.element.Edge}s where both ends are in a given set.
 **/
public class GetEdgesWithinSet extends GetElementsWithinSet<Edge> {

    public GetEdgesWithinSet() {
    }

    public GetEdgesWithinSet(final Iterable<EntitySeed> seeds) {
        super(seeds);
    }

    public GetEdgesWithinSet(final View view) {
        super(view);
    }

    public GetEdgesWithinSet(final View view, final Iterable<EntitySeed> seeds) {
        super(view, seeds);
    }

    public GetEdgesWithinSet(final GetIterableElementsOperation<EntitySeed, ?> operation) {
        super(operation);
    }

    @Override
    public void setIncludeEdges(final IncludeEdgeType includeEdges) {
        if (IncludeEdgeType.NONE == includeEdges) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " requires edges to be included");
        }

        super.setIncludeEdges(includeEdges);
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

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends AbstractGetIterableElementsOperation.BaseBuilder<GetEdgesWithinSet, EntitySeed, Edge, CHILD_CLASS> {
        public BaseBuilder() {
            super(new GetEdgesWithinSet());
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}
