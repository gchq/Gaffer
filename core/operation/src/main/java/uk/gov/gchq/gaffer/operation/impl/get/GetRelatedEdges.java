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
import uk.gov.gchq.gaffer.operation.data.ElementSeed;

/**
 * Restricts {@link uk.gov.gchq.gaffer.operation.impl.get.GetEdges} to match seeds that are related.
 * This operation takes seeds of type {@link uk.gov.gchq.gaffer.operation.data.EntitySeed} and
 * {@link uk.gov.gchq.gaffer.operation.data.EdgeSeed}.
 * <p>
 * At a basic level RELATED is defined as:
 * <ul>
 * <li>An Edge is RELATED to an EntitySeed if either the Edge's source or destination matches the EntitySeed's identifier.</li>
 * <li>An Edge is RELATED to an EdgeSeed if the Edge's seed is equal to the EdgeSeed.</li>
 * </ul>
 * <p>
 * However adjusting the includeEdge property and the incomingOutgoing property allows Edges to be filtered out.
 *
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetRelatedEdges.Builder
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetEdges
 */
@Deprecated
public class GetRelatedEdges<ELEMENT_SEED extends ElementSeed> extends GetEdges<ELEMENT_SEED> {
    public GetRelatedEdges() {
        super();
    }

    public GetRelatedEdges(final Iterable<ELEMENT_SEED> seeds) {
        super(seeds);
    }

    public GetRelatedEdges(final CloseableIterable<ELEMENT_SEED> seeds) {
        super(seeds);
    }

    public GetRelatedEdges(final View view) {
        super(view);
    }

    public GetRelatedEdges(final View view, final Iterable<ELEMENT_SEED> seeds) {
        super(view, seeds);
    }

    public GetRelatedEdges(final View view, final CloseableIterable<ELEMENT_SEED> seeds) {
        super(view, seeds);
    }

    public GetRelatedEdges(final GetIterableElementsOperation<ELEMENT_SEED, ?> operation) {
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

    public abstract static class BaseBuilder<ELEMENT_SEED extends ElementSeed, CHILD_CLASS extends BaseBuilder<ELEMENT_SEED, ?>>
            extends GetEdges.BaseBuilder<ELEMENT_SEED, CHILD_CLASS> {
        public BaseBuilder() {
            super(new GetRelatedEdges<ELEMENT_SEED>());
        }
    }

    public static final class Builder<ELEMENT_SEED extends ElementSeed>
            extends BaseBuilder<ELEMENT_SEED, Builder<ELEMENT_SEED>> {
        @Override
        protected Builder<ELEMENT_SEED> self() {
            return this;
        }
    }
}
