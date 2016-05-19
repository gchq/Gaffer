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

import gaffer.data.elementdefinition.view.View;
import gaffer.operation.AbstractGetOperation;
import gaffer.operation.GetOperation;
import gaffer.operation.data.EntitySeed;

/**
 * An <code>GetAdjacentEntitySeeds</code> operation will return the
 * {@link gaffer.operation.data.EntitySeed}s at the opposite end of connected edges to a seed
 * {@link gaffer.operation.data.EntitySeed}.
 * Seed matching is always RELATED.
 *
 * @see gaffer.operation.impl.get.GetAdjacentEntitySeeds.Builder
 * @see gaffer.operation.GetOperation
 */
public class GetAdjacentEntitySeeds extends AbstractGetOperation<EntitySeed, EntitySeed> {
    public GetAdjacentEntitySeeds() {
    }

    public GetAdjacentEntitySeeds(final Iterable<EntitySeed> seeds) {
        super(seeds);
    }

    public GetAdjacentEntitySeeds(final View view) {
        super(view);
    }

    public GetAdjacentEntitySeeds(final View view, final Iterable<EntitySeed> seeds) {
        super(view, seeds);
    }

    public GetAdjacentEntitySeeds(final GetOperation<EntitySeed, ?> operation) {
        super(operation);
    }

    @Override
    public SeedMatchingType getSeedMatching() {
        return SeedMatchingType.RELATED;
    }

    public static class Builder extends AbstractGetOperation.Builder<GetAdjacentEntitySeeds, EntitySeed, EntitySeed> {
        public Builder() {
            super(new GetAdjacentEntitySeeds());
        }

        @Override
        public Builder seeds(final Iterable<EntitySeed> seeds) {
            super.seeds(seeds);
            return this;
        }

        @Override
        public Builder addSeed(final EntitySeed seed) {
            super.addSeed(seed);
            return this;
        }

        @Override
        public Builder includeEntities(final boolean includeEntities) {
            super.includeEntities(includeEntities);
            return this;
        }

        @Override
        public Builder includeEdges(final IncludeEdgeType includeEdgeType) {
            super.includeEdges(includeEdgeType);
            return this;
        }

        @Override
        public Builder summarise(final boolean summarise) {
            super.summarise(summarise);
            return this;
        }

        @Override
        public Builder populateProperties(final boolean populateProperties) {
            super.populateProperties(populateProperties);
            return this;
        }

        @Override
        public Builder view(final View view) {
            super.view(view);
            return this;
        }

        @Override
        public Builder option(final String name, final String value) {
            super.option(name, value);
            return this;
        }
    }
}
