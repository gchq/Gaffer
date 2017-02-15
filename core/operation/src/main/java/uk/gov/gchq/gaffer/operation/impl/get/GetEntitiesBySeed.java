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

package uk.gov.gchq.gaffer.operation.impl.get;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.GetIterableElementsOperation;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;

/**
 * Restricts {@link uk.gov.gchq.gaffer.operation.impl.get.GetEntities} to match seeds that are equal.
 * This operation only takes seeds of type {@link uk.gov.gchq.gaffer.operation.data.EntitySeed}.
 * <p>
 * An Entity is EQUAL to an EntitySeed if the Entity's seed is equal to the EntitySeed.
 *
 * @see GetEntitiesBySeed.Builder
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetEntities
 */
@Deprecated
public class GetEntitiesBySeed extends GetEntities<EntitySeed> {
    public GetEntitiesBySeed() {
        super();
    }

    public GetEntitiesBySeed(final Iterable<EntitySeed> seeds) {
        super(seeds);
    }

    public GetEntitiesBySeed(final CloseableIterable<EntitySeed> seeds) {
        super(seeds);
    }

    public GetEntitiesBySeed(final View view) {
        super(view);
    }

    public GetEntitiesBySeed(final View view, final Iterable<EntitySeed> seeds) {
        super(view, seeds);
    }

    public GetEntitiesBySeed(final View view, final CloseableIterable<EntitySeed> seeds) {
        super(view, seeds);
    }

    public GetEntitiesBySeed(final GetIterableElementsOperation<EntitySeed, ?> operation) {
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

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends GetEntities.BaseBuilder<EntitySeed, BaseBuilder<CHILD_CLASS>> {
        public BaseBuilder() {
            super(new GetEntitiesBySeed());
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected BaseBuilder<Builder> self() {
            return this;
        }
    }
}
