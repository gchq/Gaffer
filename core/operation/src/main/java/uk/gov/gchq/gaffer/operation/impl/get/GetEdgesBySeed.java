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
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;

/**
 * Restricts {@link uk.gov.gchq.gaffer.operation.impl.get.GetEdges} to match seeds that are equal.
 * This operation only takes seeds of type {@link uk.gov.gchq.gaffer.operation.data.EdgeSeed}.
 * <p>
 * At a basic level an Edge is EQUAL to an EdgeSeed if the Edge's seed is equal to the EdgeSeed.
 * However adjusting the includeEdge property allows for some Edges to be filtered out.
 *
 * @see GetEdgesBySeed.Builder
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetEdges
 */
@Deprecated
public class GetEdgesBySeed extends GetEdges<EdgeSeed> {
    public GetEdgesBySeed() {
        super();
    }

    public GetEdgesBySeed(final Iterable<EdgeSeed> seeds) {
        super(seeds);
    }

    public GetEdgesBySeed(final CloseableIterable<EdgeSeed> seeds) {
        super(seeds);
    }

    public GetEdgesBySeed(final View view) {
        super(view);
    }

    public GetEdgesBySeed(final View view, final Iterable<EdgeSeed> seeds) {
        super(view, seeds);
    }

    public GetEdgesBySeed(final View view, final CloseableIterable<EdgeSeed> seeds) {
        super(view, seeds);
    }

    public GetEdgesBySeed(final GetIterableElementsOperation<EdgeSeed, ?> operation) {
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
            extends GetEdges.BaseBuilder<EdgeSeed, CHILD_CLASS> {
        public BaseBuilder() {
            super(new GetEdgesBySeed());
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}
