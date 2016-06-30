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
import gaffer.operation.AbstractGetOperation;
import gaffer.operation.GetOperation;
import gaffer.operation.data.ElementSeed;

/**
 * Returns all {@link gaffer.data.element.Edge}'s between the provided
 * {@link gaffer.operation.data.ElementSeed}s.
 */
public class GetEdgesInRanges<SEED_TYPE extends Pair<? extends ElementSeed>> extends GetElementsInRanges<SEED_TYPE, Edge> {

    public GetEdgesInRanges() {
    }

    public GetEdgesInRanges(final Iterable<SEED_TYPE> seeds) {
        super(seeds);
    }

    public GetEdgesInRanges(final View view) {
        super(view);
    }

    public GetEdgesInRanges(final View view, final Iterable<SEED_TYPE> seeds) {
        super(view, seeds);
    }

    public GetEdgesInRanges(final GetOperation<SEED_TYPE, Edge> operation) {
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

    public static class Builder<SEED_TYPE extends Pair<? extends ElementSeed>>
            extends AbstractGetOperation.Builder<GetEdgesInRanges<SEED_TYPE>, SEED_TYPE, Edge> {

        public Builder() {
            super(new GetEdgesInRanges());
        }

        @Override
        public Builder<SEED_TYPE> summarise(final boolean summarise) {
            return (Builder<SEED_TYPE>) super.summarise(summarise);
        }

        @Override
        public Builder<SEED_TYPE> deduplicate(final boolean deduplicate) {
            return (Builder<SEED_TYPE>) super.deduplicate(deduplicate);
        }

        @Override
        public Builder<SEED_TYPE> populateProperties(final boolean populateProperties) {
            return (Builder<SEED_TYPE>) super.populateProperties(populateProperties);
        }

        @Override
        public Builder<SEED_TYPE> view(final View view) {
            return (Builder<SEED_TYPE>) super.view(view);
        }

        @Override
        public Builder<SEED_TYPE> option(final String name, final String value) {
            return (Builder<SEED_TYPE>) super.option(name, value);
        }

        @Override
        public Builder<SEED_TYPE> seeds(final Iterable<SEED_TYPE> newSeeds) {
            return (Builder<SEED_TYPE>) super.seeds(newSeeds);
        }

        @Override
        public Builder<SEED_TYPE> addSeed(final SEED_TYPE seed) {
            return (Builder<SEED_TYPE>) super.addSeed(seed);
        }

        @Override
        public Builder<SEED_TYPE> includeEdges(final IncludeEdgeType includeEdgeType) {
            return (Builder<SEED_TYPE>) super.includeEdges(includeEdgeType);
        }

        @Override
        public Builder<SEED_TYPE> inOutType(final IncludeIncomingOutgoingType inOutType) {
            return (Builder<SEED_TYPE>) super.inOutType(inOutType);
        }
    }

}
