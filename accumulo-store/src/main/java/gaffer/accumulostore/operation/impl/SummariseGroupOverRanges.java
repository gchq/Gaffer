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
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.AbstractGetOperation;
import gaffer.operation.data.ElementSeed;

/**
 * A <code>SummariseGroupOverRanges</code> operation will return an
 * {@link gaffer.data.element.Element} that represents the aggregated form of all data between the provided range for the provided group.
 * Note that one result per tablet on which data in the desired range resides will be returned, with large data sets and/or large ranges
 * more likely to produce multiple results and you will need to cache the results and aggregate them again to get a final answer.
 *
 * For this reason it is recommended your provided ranges do not over-lap as you will be unable to tell for a given result which range the result is from.
 *
 * Standard filtering will still occur before the final aggregation of the vertices.
 * @see gaffer.operation.GetOperation
 */
public class SummariseGroupOverRanges<SEED_TYPE extends Pair<? extends ElementSeed>, ELEMENT_TYPE extends Element> extends GetElementsInRanges<SEED_TYPE, ELEMENT_TYPE> {

    public SummariseGroupOverRanges() {
    }

    public SummariseGroupOverRanges(final View view, final Iterable<SEED_TYPE> seeds) {
        super(view, seeds);
    }

    public static class Builder<SEED_TYPE extends Pair<? extends ElementSeed>, ELEMENT_TYPE extends Element>
            extends AbstractGetOperation.Builder<SummariseGroupOverRanges<SEED_TYPE, ELEMENT_TYPE>, SEED_TYPE, ELEMENT_TYPE> {
        public Builder() {
            super(new SummariseGroupOverRanges());
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> summarise(final boolean summarise) {
            return (Builder<SEED_TYPE, ELEMENT_TYPE>) super.summarise(summarise);
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> populateProperties(final boolean populateProperties) {
            return (Builder<SEED_TYPE, ELEMENT_TYPE>) super.populateProperties(populateProperties);
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> view(final View view) {
            return (Builder<SEED_TYPE, ELEMENT_TYPE>) super.view(view);
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> option(final String name, final String value) {
            return (Builder<SEED_TYPE, ELEMENT_TYPE>) super.option(name, value);
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> seeds(final Iterable<SEED_TYPE> newSeeds) {
            return (Builder<SEED_TYPE, ELEMENT_TYPE>) super.seeds(newSeeds);
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> addSeed(final SEED_TYPE seed) {
            return (Builder<SEED_TYPE, ELEMENT_TYPE>) super.addSeed(seed);
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> includeEntities(final boolean includeEntities) {
            return (Builder<SEED_TYPE, ELEMENT_TYPE>) super.includeEntities(includeEntities);
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> includeEdges(final IncludeEdgeType includeEdgeType) {
            return (Builder<SEED_TYPE, ELEMENT_TYPE>) super.includeEdges(includeEdgeType);
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> inOutType(final IncludeIncomingOutgoingType inOutType) {
            return (Builder<SEED_TYPE, ELEMENT_TYPE>) super.inOutType(inOutType);
        }
    }
}
