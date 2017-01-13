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
import uk.gov.gchq.gaffer.operation.AbstractGetIterableElementsOperation;
import uk.gov.gchq.gaffer.operation.GetIterableElementsOperation;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

/**
 * An <code>GetAdjacentEntitySeeds</code> operation will return the
 * {@link uk.gov.gchq.gaffer.operation.data.EntitySeed}s at the opposite end of connected edges to a seed
 * {@link uk.gov.gchq.gaffer.operation.data.EntitySeed}.
 * Seed matching is always RELATED.
 *
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds.Builder
 * @see uk.gov.gchq.gaffer.operation.GetOperation
 */
public class GetAdjacentEntitySeeds extends AbstractGetIterableElementsOperation<EntitySeed, EntitySeed> {
    public GetAdjacentEntitySeeds() {
        setOutputTypeReference(new TypeReferenceImpl.CloseableIterableEntitySeed());
    }

    public GetAdjacentEntitySeeds(final Iterable<EntitySeed> seeds) {
        super(seeds);
        setOutputTypeReference(new TypeReferenceImpl.CloseableIterableEntitySeed());
    }

    public GetAdjacentEntitySeeds(final CloseableIterable<EntitySeed> seeds) {
        super(seeds);
        setOutputTypeReference(new TypeReferenceImpl.CloseableIterableEntitySeed());
    }

    public GetAdjacentEntitySeeds(final View view) {
        super(view);
        setOutputTypeReference(new TypeReferenceImpl.CloseableIterableEntitySeed());
    }

    public GetAdjacentEntitySeeds(final View view, final Iterable<EntitySeed> seeds) {
        super(view, seeds);
        setOutputTypeReference(new TypeReferenceImpl.CloseableIterableEntitySeed());
    }

    public GetAdjacentEntitySeeds(final View view, final CloseableIterable<EntitySeed> seeds) {
        super(view, seeds);
        setOutputTypeReference(new TypeReferenceImpl.CloseableIterableEntitySeed());
    }

    public GetAdjacentEntitySeeds(final GetIterableElementsOperation<EntitySeed, ?> operation) {
        super(operation);
        setOutputTypeReference(new TypeReferenceImpl.CloseableIterableEntitySeed());
    }

    @Override
    public SeedMatchingType getSeedMatching() {
        return SeedMatchingType.RELATED;
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends AbstractGetIterableElementsOperation.BaseBuilder<GetAdjacentEntitySeeds, EntitySeed, EntitySeed, CHILD_CLASS> {
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
