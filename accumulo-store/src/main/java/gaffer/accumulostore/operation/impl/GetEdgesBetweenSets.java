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

import gaffer.accumulostore.operation.AbstractAccumuloTwoSetSeededOperation;
import gaffer.data.element.Edge;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.data.EntitySeed;

/**
 * Given two sets of {@link gaffer.operation.data.EntitySeed}s, called A and B,
 * this retrieves all {@link gaffer.data.element.Edge}s where one end is in set
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

    public static class Builder
            extends AbstractAccumuloTwoSetSeededOperation.Builder<GetEdgesBetweenSets, EntitySeed, Edge> {

        public Builder() {
            super(new GetEdgesBetweenSets());
        }

        @Override
        public Builder seedsB(final Iterable<EntitySeed> seedsB) {
            return (Builder) super.seedsB(seedsB);
        }

        @Override
        public Builder addSeedB(final EntitySeed seed) {
            return (Builder) super.addSeedB(seed);
        }

        @Override
        public Builder seeds(final Iterable<EntitySeed> newSeeds) {
            return (Builder) super.seeds(newSeeds);
        }

        @Override
        public Builder addSeed(final EntitySeed seed) {
            return (Builder) super.addSeed(seed);
        }

        @Override
        public Builder summarise(final boolean summarise) {
            return (Builder) super.summarise(summarise);
        }

        @Override
        public Builder deduplicate(final boolean deduplicate) {
            return (Builder) super.deduplicate(deduplicate);
        }

        @Override
        public Builder populateProperties(final boolean populateProperties) {
            return (Builder) super.populateProperties(populateProperties);
        }

        @Override
        public Builder view(final View view) {
            return (Builder) super.view(view);
        }

        @Override
        public Builder option(final String name, final String value) {
            return (Builder) super.option(name, value);
        }

        @Override
        public Builder includeEdges(final IncludeEdgeType includeEdgeType) {
            return (Builder) super.includeEdges(includeEdgeType);
        }

        @Override
        public Builder inOutType(final IncludeIncomingOutgoingType inOutType) {
            return (Builder) super.inOutType(inOutType);
        }
    }

}
