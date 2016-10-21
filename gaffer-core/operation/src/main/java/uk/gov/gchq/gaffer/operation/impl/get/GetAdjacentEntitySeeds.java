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
import uk.gov.gchq.gaffer.operation.AbstractGetOperation;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;

/**
 * An <code>GetAdjacentEntitySeeds</code> operation will return the
 * {@link gaffer.operation.data.EntitySeed}s at the opposite end of connected edges to a seed
 * {@link gaffer.operation.data.EntitySeed}.
 * Seed matching is always RELATED.
 *
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds.Builder
 * @see uk.gov.gchq.gaffer.operation.GetOperation
 */
public class GetAdjacentEntitySeeds extends AbstractGetOperation<EntitySeed, CloseableIterable<EntitySeed>> {
    public GetAdjacentEntitySeeds() {
    }

    public GetAdjacentEntitySeeds(final Iterable<EntitySeed> seeds) {
        super(seeds);
    }

    public GetAdjacentEntitySeeds(final CloseableIterable<EntitySeed> seeds) {
        super(seeds);
    }

    public GetAdjacentEntitySeeds(final View view) {
        super(view);
    }

    public GetAdjacentEntitySeeds(final View view, final Iterable<EntitySeed> seeds) {
        super(view, seeds);
    }

    public GetAdjacentEntitySeeds(final View view, final CloseableIterable<EntitySeed> seeds) {
        super(view, seeds);
    }

    public GetAdjacentEntitySeeds(final GetOperation<EntitySeed, ?> operation) {
        super(operation);
    }

    @Override
    public SeedMatchingType getSeedMatching() {
        return SeedMatchingType.RELATED;
    }
    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends AbstractGetOperation.BaseBuilder<GetAdjacentEntitySeeds, EntitySeed, CloseableIterable<EntitySeed>, CHILD_CLASS> {
        public BaseBuilder() {
            super(new GetAdjacentEntitySeeds());
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}
