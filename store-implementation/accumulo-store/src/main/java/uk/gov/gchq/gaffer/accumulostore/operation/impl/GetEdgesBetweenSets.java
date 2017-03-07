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

import uk.gov.gchq.gaffer.accumulostore.operation.AbstractAccumuloTwoSetSeededOperation;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;

/**
 * Given two sets of {@link uk.gov.gchq.gaffer.operation.data.EntitySeed}s, called A and B,
 * this retrieves all {@link uk.gov.gchq.gaffer.data.element.Edge}s where one end is in set
 * A and the other is in set B.
 */
public class GetEdgesBetweenSets extends GetElementsBetweenSets<Edge> {

    public GetEdgesBetweenSets() {
    }

    public GetEdgesBetweenSets(final Iterable<EntitySeed> seedsA, final Iterable<EntitySeed> seedsB) {
        super(seedsA, seedsB);
        super.setIncludeEdges(IncludeEdgeType.ALL);
    }

    public GetEdgesBetweenSets(final Iterable<EntitySeed> seedsA, final Iterable<EntitySeed> seedsB, final View view) {
        super(seedsA, seedsB, view);
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

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends AbstractAccumuloTwoSetSeededOperation.BaseBuilder<GetEdgesBetweenSets, EntitySeed, Edge, CHILD_CLASS> {

        public BaseBuilder() {
            super(new GetEdgesBetweenSets());
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}
