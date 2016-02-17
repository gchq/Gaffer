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

import gaffer.operation.data.EntitySeed;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.GetOperation;

/**
 * Restricts {@link gaffer.operation.impl.get.GetEntities} to match seeds that are equal.
 * This operation only takes seeds of type {@link gaffer.operation.data.EntitySeed}.
 * <p>
 * An Entity is EQUAL to an EntitySeed if the Entity's seed is equal to the EntitySeed.
 *
 * @see GetEntitiesBySeed.Builder
 * @see gaffer.operation.impl.get.GetEntities
 */
public class GetEntitiesBySeed extends GetEntities<EntitySeed> {
    public GetEntitiesBySeed() {
        super();
    }

    public GetEntitiesBySeed(final Iterable<EntitySeed> seeds) {
        super(seeds);
    }

    public GetEntitiesBySeed(final View view) {
        super(view);
    }

    public GetEntitiesBySeed(final View view, final Iterable<EntitySeed> seeds) {
        super(view, seeds);
    }

    public GetEntitiesBySeed(final GetOperation<EntitySeed, ?> operation) {
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

    public static class Builder extends GetEntities.Builder<GetEntitiesBySeed, EntitySeed> {
        public Builder() {
            super(new GetEntitiesBySeed());
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
