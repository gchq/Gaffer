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
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.data.EntitySeed;

/**
 * Given two sets of {@link gaffer.operation.data.EntitySeed}s, called A and B,
 * this retrieves all {@link gaffer.data.element.Edge}s where one end is in set
 * A and the other is in set B and also returns
 * {@link gaffer.data.element.Entity}s for
 * {@link gaffer.operation.data.EntitySeed}s in set A.
 */
public class GetElementsBetweenSets<ELEMENT_TYPE extends Element>
        extends AbstractAccumuloTwoSetSeededOperation<EntitySeed, ELEMENT_TYPE> {

    public GetElementsBetweenSets() {
    }

    public GetElementsBetweenSets(final Iterable<EntitySeed> seedsA, final Iterable<EntitySeed> seedsB) {
        super(seedsA, seedsB);
    }

    public GetElementsBetweenSets(final Iterable<EntitySeed> seedsA, final Iterable<EntitySeed> seedsB,
                                  final View view) {
        super(seedsA, seedsB, view);
    }

    public static class Builder<OP_TYPE extends GetElementsBetweenSets<ELEMENT_TYPE>, ELEMENT_TYPE extends Element>
            extends AbstractAccumuloTwoSetSeededOperation.Builder<OP_TYPE, EntitySeed, ELEMENT_TYPE> {

        public Builder(final OP_TYPE op) {
            super(op);
        }

        @Override
        public Builder<OP_TYPE, ELEMENT_TYPE> seedsB(final Iterable<EntitySeed> seedsB) {
            return (Builder<OP_TYPE, ELEMENT_TYPE>) super.seedsB(seedsB);
        }

        @Override
        public Builder<OP_TYPE, ELEMENT_TYPE> addSeedB(final EntitySeed seed) {
            return (Builder<OP_TYPE, ELEMENT_TYPE>) super.addSeedB(seed);
        }

        @Override
        public Builder<OP_TYPE, ELEMENT_TYPE> seeds(final Iterable<EntitySeed> newSeeds) {
            return (Builder<OP_TYPE, ELEMENT_TYPE>) super.seeds(newSeeds);
        }

        @Override
        public Builder<OP_TYPE, ELEMENT_TYPE> addSeed(final EntitySeed seed) {
            return (Builder<OP_TYPE, ELEMENT_TYPE>) super.addSeed(seed);
        }

        @Override
        public Builder<OP_TYPE, ELEMENT_TYPE> summarise(final boolean summarise) {
            return (Builder<OP_TYPE, ELEMENT_TYPE>) super.summarise(summarise);
        }

        @Override
        public Builder<OP_TYPE, ELEMENT_TYPE> populateProperties(final boolean populateProperties) {
            return (Builder<OP_TYPE, ELEMENT_TYPE>) super.populateProperties(populateProperties);
        }

        @Override
        public Builder<OP_TYPE, ELEMENT_TYPE> view(final View view) {
            return (Builder<OP_TYPE, ELEMENT_TYPE>) super.view(view);
        }

        @Override
        public Builder<OP_TYPE, ELEMENT_TYPE> option(final String name, final String value) {
            return (Builder<OP_TYPE, ELEMENT_TYPE>) super.option(name, value);
        }

        @Override
        public Builder<OP_TYPE, ELEMENT_TYPE> includeEntities(final boolean includeEntities) {
            return (Builder<OP_TYPE, ELEMENT_TYPE>) super.includeEntities(includeEntities);
        }

        @Override
        public Builder<OP_TYPE, ELEMENT_TYPE> includeEdges(final IncludeEdgeType includeEdgeType) {
            return (Builder<OP_TYPE, ELEMENT_TYPE>) super.includeEdges(includeEdgeType);
        }

        @Override
        public Builder<OP_TYPE, ELEMENT_TYPE> inOutType(final IncludeIncomingOutgoingType inOutType) {
            return (Builder<OP_TYPE, ELEMENT_TYPE>) super.inOutType(inOutType);
        }
    }
}
