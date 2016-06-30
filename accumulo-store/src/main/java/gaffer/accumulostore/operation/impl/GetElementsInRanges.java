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
import gaffer.operation.GetOperation;
import gaffer.operation.data.ElementSeed;

/**
 * This returns all data between the provided
 * {@link gaffer.operation.data.ElementSeed}s.
 */
public class GetElementsInRanges<SEED_TYPE extends Pair<? extends ElementSeed>, ELEMENT_TYPE extends Element>
        extends AbstractGetOperation<SEED_TYPE, ELEMENT_TYPE> {

    public GetElementsInRanges() {
    }

    public GetElementsInRanges(final Iterable<SEED_TYPE> seeds) {
        super(seeds);
    }

    public GetElementsInRanges(final View view) {
        super(view);
    }

    public GetElementsInRanges(final View view, final Iterable<SEED_TYPE> seeds) {
        super(view, seeds);
    }

    public GetElementsInRanges(final GetOperation<SEED_TYPE, ?> operation) {
        super(operation);
    }

    public static class Builder<SEED_TYPE extends Pair<? extends ElementSeed>, ELEMENT_TYPE extends Element>
            extends AbstractGetOperation.Builder<GetElementsInRanges<SEED_TYPE, ELEMENT_TYPE>, SEED_TYPE, ELEMENT_TYPE> {
        public Builder() {
            super(new GetElementsInRanges());
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> summarise(final boolean summarise) {
            return (Builder<SEED_TYPE, ELEMENT_TYPE>) super.summarise(summarise);
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> deduplicate(final boolean deduplicate) {
            return (Builder<SEED_TYPE, ELEMENT_TYPE>) super.deduplicate(deduplicate);
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
