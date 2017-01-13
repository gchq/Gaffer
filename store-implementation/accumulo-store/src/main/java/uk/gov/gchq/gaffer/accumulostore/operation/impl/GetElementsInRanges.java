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

package uk.gov.gchq.gaffer.accumulostore.operation.impl;

import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.AbstractGetIterableElementsOperation;
import uk.gov.gchq.gaffer.operation.GetIterableElementsOperation;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;

/**
 * This returns all data between the provided
 * {@link uk.gov.gchq.gaffer.operation.data.ElementSeed}s.
 */
public class GetElementsInRanges<SEED_TYPE extends Pair<? extends ElementSeed>, ELEMENT_TYPE extends Element>
        extends AbstractGetIterableElementsOperation<SEED_TYPE, ELEMENT_TYPE> {

    public GetElementsInRanges() {
    }

    public GetElementsInRanges(final Iterable<SEED_TYPE> seeds) {
        super(seeds);
    }

    public GetElementsInRanges(final View view) {
        super(view);
    }

    public GetElementsInRanges(final View view, final Iterable<SEED_TYPE> seeds) {
        super(view, seeds);
    }

    public GetElementsInRanges(final GetIterableElementsOperation<SEED_TYPE, ?> operation) {
        super(operation);
    }

    public abstract static class BaseBuilder<SEED_TYPE extends Pair<? extends ElementSeed>,
            ELEMENT_TYPE extends Element,
            CHILD_CLASS extends BaseBuilder<SEED_TYPE, ELEMENT_TYPE, ?>>
            extends AbstractGetIterableElementsOperation.BaseBuilder<GetElementsInRanges<SEED_TYPE, ELEMENT_TYPE>, SEED_TYPE, ELEMENT_TYPE, CHILD_CLASS> {
        public BaseBuilder() {
            super(new GetElementsInRanges());
        }
    }

    public static final class Builder<SEED_TYPE extends Pair<? extends ElementSeed>,
            ELEMENT_TYPE extends Element>
            extends BaseBuilder<SEED_TYPE, ELEMENT_TYPE, Builder<SEED_TYPE, ELEMENT_TYPE>> {

        @Override
        protected Builder<SEED_TYPE, ELEMENT_TYPE> self() {
            return this;
        }
    }
}
