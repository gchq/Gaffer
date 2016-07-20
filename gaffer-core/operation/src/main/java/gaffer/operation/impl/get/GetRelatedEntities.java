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
import gaffer.operation.GetOperation;
import gaffer.operation.data.ElementSeed;

/**
 * Restricts {@link gaffer.operation.impl.get.GetEntities} to match seeds that are related.
 * This operation takes seeds of type {@link gaffer.operation.data.EntitySeed} and
 * {@link gaffer.operation.data.EdgeSeed}.
 * <p>
 * At a basic level RELATED is defined as:
 * <ul>
 * <li>An Entity is RELATED to an EntitySeed if the Entity's seed is equal to the EntitySeed.</li>
 * <li>An Entity is RELATED to an EdgeSeed if either the Entity's seed is equal to either the EdgeSeed's source or destination.</li>
 * </ul>
 * However adjusting the includeEdge property and the incomingOutgoing property allows Entities to be filtered out.
 * <p>
 * For example:
 * <pre>{@code
 *  new GetRelatedEntities.Builder<EdgeSeed>()
 *      .addSeed(new EdgeSeed(1, 2, true))
 *      .build();
 *  new GetRelatedEntities.Builder<EdgeSeed>()
 *      .addSeed(new EdgeSeed(1, 2, true))
 *      .view(new View.Builder()
 *          .entity("entity", new ViewElementDefinition.Builder()
 *              .filter(new ElementFilter.Builder()
 *                  .select(COUNT)
 *                  .execute(new IsMoreThan(1))
 *                  .build())
 *              .build())
 *          .build())
 *      .build();
 * }</pre>
 *
 * @see gaffer.operation.impl.get.GetRelatedEntities.Builder
 * @see gaffer.operation.impl.get.GetEntities
 */
public class GetRelatedEntities<ELEMENT_SEED extends ElementSeed> extends GetEntities<ELEMENT_SEED> {
    public GetRelatedEntities() {
        super();
    }

    public GetRelatedEntities(final Iterable<ELEMENT_SEED> seeds) {
        super(seeds);
    }

    public GetRelatedEntities(final View view) {
        super(view);
    }

    public GetRelatedEntities(final View view, final Iterable<ELEMENT_SEED> seeds) {
        super(view, seeds);
    }

    public GetRelatedEntities(final GetOperation<ELEMENT_SEED, ?> operation) {
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
        return SeedMatchingType.RELATED;
    }

    public static class Builder<ELEMENT_SEED extends ElementSeed> extends GetEntities.Builder<GetRelatedEntities<ELEMENT_SEED>, ELEMENT_SEED> {
        public Builder() {
            super(new GetRelatedEntities<ELEMENT_SEED>());
        }

        @Override
        public Builder<ELEMENT_SEED> seeds(final Iterable<ELEMENT_SEED> seeds) {
            super.seeds(seeds);
            return this;
        }

        @Override
        public Builder<ELEMENT_SEED> addSeed(final ELEMENT_SEED seed) {
            super.addSeed(seed);
            return this;
        }

        @Override
        public Builder<ELEMENT_SEED> summarise(final boolean summarise) {
            super.summarise(summarise);
            return this;
        }

        @Override
        public Builder<ELEMENT_SEED> deduplicate(final boolean deduplicate) {
            return (Builder<ELEMENT_SEED>) super.deduplicate(deduplicate);
        }

        @Override
        public Builder<ELEMENT_SEED> populateProperties(final boolean populateProperties) {
            super.populateProperties(populateProperties);
            return this;
        }

        @Override
        public Builder<ELEMENT_SEED> view(final View view) {
            super.view(view);
            return this;
        }

        @Override
        public Builder<ELEMENT_SEED> option(final String name, final String value) {
            super.option(name, value);
            return this;
        }
    }
}
