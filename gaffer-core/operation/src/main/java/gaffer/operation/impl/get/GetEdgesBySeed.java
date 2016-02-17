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

import gaffer.operation.data.EdgeSeed;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.GetOperation;

/**
 * Restricts {@link gaffer.operation.impl.get.GetEdges} to match seeds that are equal.
 * This operation only takes seeds of type {@link gaffer.operation.data.EdgeSeed}.
 * <p>
 * At a basic level an Edge is EQUAL to an EdgeSeed if the Edge's seed is equal to the EdgeSeed.
 * However adjusting the includeEdge property allows for some Edges to be filtered out.
 *
 * @see GetEdgesBySeed.Builder
 * @see gaffer.operation.impl.get.GetEdges
 */
public class GetEdgesBySeed extends GetEdges<EdgeSeed> {
    public GetEdgesBySeed() {
        super();
    }

    public GetEdgesBySeed(final Iterable<EdgeSeed> seeds) {
        super(seeds);
    }

    public GetEdgesBySeed(final View view) {
        super(view);
    }

    public GetEdgesBySeed(final View view, final Iterable<EdgeSeed> seeds) {
        super(view, seeds);
    }

    public GetEdgesBySeed(final GetOperation<EdgeSeed, ?> operation) {
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

    public static class Builder extends GetEdges.Builder<GetEdgesBySeed, EdgeSeed> {
        public Builder() {
            super(new GetEdgesBySeed());
        }

        @Override
        public Builder seeds(final Iterable<EdgeSeed> seeds) {
            super.seeds(seeds);
            return this;
        }

        @Override
        public Builder addSeed(final EdgeSeed seed) {
            super.addSeed(seed);
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
