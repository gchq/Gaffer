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

import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.GetOperation;
import gaffer.operation.data.ElementSeed;

/**
 * Restricts {@link gaffer.operation.impl.get.GetElements} to match seeds that are equal.
 * <p>
 * At a basic level EQUAL is defined as:
 * <ul>
 * <li>An Entity is EQUAL to an EntitySeed if the Entity's seed is equal to the EntitySeed.</li>
 * <li>An Entity is never EQUAL to an EdgeSeed.
 * <li>An Edge is EQUAL to an EdgeSeed if the Edge's seed is equal to the EdgeSeed.
 * <li>An Edge is never EQUAL to an EntitySeed.</li>
 * </ul>
 * However adjusting the includeEdge property allows for some Edges to be filtered out.
 *
 * @param <SEED_TYPE>    the seed seed type
 * @param <ELEMENT_TYPE> the element return type
 * @see GetElementsSeed.Builder
 * @see gaffer.operation.impl.get.GetElements
 */
public class GetElementsSeed<SEED_TYPE extends ElementSeed, ELEMENT_TYPE extends Element>
        extends GetElements<SEED_TYPE, ELEMENT_TYPE> {
    public GetElementsSeed() {
        super();
    }

    public GetElementsSeed(final Iterable<SEED_TYPE> seeds) {
        super(seeds);
    }

    public GetElementsSeed(final View view) {
        super(view);
    }

    public GetElementsSeed(final View view, final Iterable<SEED_TYPE> seeds) {
        super(view, seeds);
    }

    public GetElementsSeed(final GetOperation<SEED_TYPE, ?> operation) {
        super(operation);
    }

    @Override
    public void setSeedMatching(final SeedMatchingType seedMatching) {
        if (!getSeedMatching().equals(seedMatching)) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " only supports seed matching when set to " + getSeedMatching().name());
        }
    }

    @Override
    public SeedMatchingType getSeedMatching() {
        return SeedMatchingType.EQUAL;
    }

    public static class Builder<SEED_TYPE extends ElementSeed, ELEMENT_TYPE extends Element>
            extends GetElements.Builder<GetElementsSeed<SEED_TYPE, ELEMENT_TYPE>, SEED_TYPE, ELEMENT_TYPE> {
        public Builder() {
            super(new GetElementsSeed<SEED_TYPE, ELEMENT_TYPE>());
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> seeds(final Iterable<SEED_TYPE> seeds) {
            super.seeds(seeds);
            return this;
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> addSeed(final SEED_TYPE seed) {
            super.addSeed(seed);
            return this;
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> includeEntities(final boolean includeEntities) {
            super.includeEntities(includeEntities);
            return this;
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> includeEdges(final IncludeEdgeType includeEdgeType) {
            super.includeEdges(includeEdgeType);
            return this;
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> summarise(final boolean summarise) {
            super.summarise(summarise);
            return this;
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> populateProperties(final boolean populateProperties) {
            super.populateProperties(populateProperties);
            return this;
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> view(final View view) {
            super.view(view);
            return this;
        }

        @Override
        public Builder<SEED_TYPE, ELEMENT_TYPE> option(final String name, final String value) {
            super.option(name, value);
            return this;
        }
    }
}
