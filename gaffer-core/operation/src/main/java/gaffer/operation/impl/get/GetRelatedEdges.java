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

import gaffer.operation.data.ElementSeed;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.GetOperation;

/**
 * Restricts {@link gaffer.operation.impl.get.GetEdges} to match seeds that are related.
 * This operation takes seeds of type {@link gaffer.operation.data.EntitySeed} and
 * {@link gaffer.operation.data.EdgeSeed}.
 * <p>
 * At a basic level RELATED is defined as:
 * <ul>
 * <li>An Edge is RELATED to an EntitySeed if either the Edge's source or destination matches the EntitySeed's identifier.</li>
 * <li>An Edge is RELATED to an EdgeSeed if the Edge's seed is equal to the EdgeSeed.</li>
 * </ul>
 * <p>
 * However adjusting the includeEdge property and the incomingOutgoing property allows Edges to be filtered out.
 *
 * @see gaffer.operation.impl.get.GetRelatedEdges.Builder
 * @see gaffer.operation.impl.get.GetEdges
 */
public class GetRelatedEdges extends GetEdges<ElementSeed> {
    public GetRelatedEdges() {
        super();
    }

    public GetRelatedEdges(final Iterable<ElementSeed> seeds) {
        super(seeds);
    }

    public GetRelatedEdges(final View view) {
        super(view);
    }

    public GetRelatedEdges(final View view, final Iterable<ElementSeed> seeds) {
        super(view, seeds);
    }

    public GetRelatedEdges(final GetOperation<ElementSeed, ?> operation) {
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

    public static class Builder extends GetEdges.Builder<GetRelatedEdges, ElementSeed> {
        public Builder() {
            super(new GetRelatedEdges());
        }

        @Override
        public Builder seeds(final Iterable<ElementSeed> seeds) {
            super.seeds(seeds);
            return this;
        }

        @Override
        public Builder addSeed(final ElementSeed seed) {
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
