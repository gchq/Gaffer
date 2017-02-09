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
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.GetIterableElementsOperation;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;

/**
 * Restricts {@link uk.gov.gchq.gaffer.operation.impl.get.GetElements} to match seeds that are equal.
 * <p>
 * At a basic level EQUAL is defined as:
 * <ul>
 * <li>An Entity is EQUAL to an EntitySeed if the Entity's seed is equal to the EntitySeed.</li>
 * <li>An Entity is never EQUAL to an EdgeSeed.
 * <li>An Edge is EQUAL to an EdgeSeed if the Edge's seed is equal to the EdgeSeed.
 * <li>An Edge is never EQUAL to an EntitySeed.</li>
 * </ul>
 * However adjusting the includeEdge property allows for some Edges to be filtered out.
 *
 * @param <SEED_TYPE>    the seed seed type
 * @param <ELEMENT_TYPE> the element return type
 * @see GetElementsBySeed.Builder
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetElements
 */
@Deprecated
public class GetElementsBySeed<SEED_TYPE extends ElementSeed, ELEMENT_TYPE extends Element>
        extends GetElements<SEED_TYPE, ELEMENT_TYPE> {
    public GetElementsBySeed() {
        super();
    }

    public GetElementsBySeed(final Iterable<SEED_TYPE> seeds) {
        super(seeds);
    }

    public GetElementsBySeed(final CloseableIterable<SEED_TYPE> seeds) {
        super(seeds);
    }

    public GetElementsBySeed(final View view) {
        super(view);
    }

    public GetElementsBySeed(final View view, final Iterable<SEED_TYPE> seeds) {
        super(view, seeds);
    }

    public GetElementsBySeed(final View view, final CloseableIterable<SEED_TYPE> seeds) {
        super(view, seeds);
    }

    public GetElementsBySeed(final GetIterableElementsOperation<SEED_TYPE, ?> operation) {
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

    public abstract static class BaseBuilder<SEED_TYPE extends ElementSeed,
            ELEMENT_TYPE extends Element,
            CHILD_CLASS extends BaseBuilder<SEED_TYPE, ELEMENT_TYPE, ?>>
            extends GetElements.BaseBuilder<GetElementsBySeed<SEED_TYPE, ELEMENT_TYPE>, SEED_TYPE, ELEMENT_TYPE, CHILD_CLASS> {
        public BaseBuilder() {
            super(new GetElementsBySeed<SEED_TYPE, ELEMENT_TYPE>());
        }
    }

    public static final class Builder<SEED_TYPE extends ElementSeed, ELEMENT_TYPE extends Element>
            extends BaseBuilder<SEED_TYPE, ELEMENT_TYPE, Builder<SEED_TYPE, ELEMENT_TYPE>> {

        @Override
        protected Builder<SEED_TYPE, ELEMENT_TYPE> self() {
            return this;
        }
    }
}
