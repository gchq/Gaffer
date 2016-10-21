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
import uk.gov.gchq.gaffer.operation.AbstractGetOperation;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;

/**
 * Restricts {@link gaffer.operation.AbstractGetOperation} to take {@link gaffer.operation.data.ElementSeed}s as
 * seeds and returns {@link gaffer.data.element.Element}s
 * There are various flags to filter out the elements returned. See implementations of {@link GetElements} for further details.
 *
 * @param <SEED_TYPE>    the seed seed type
 * @param <ELEMENT_TYPE> the element return type
 * @see uk.gov.gchq.gaffer.operation.GetOperation
 */
public abstract class GetElements<SEED_TYPE extends ElementSeed, ELEMENT_TYPE extends Element>
        extends AbstractGetOperation<SEED_TYPE, CloseableIterable<ELEMENT_TYPE>> {
    public GetElements() {
        super();
    }

    public GetElements(final Iterable<SEED_TYPE> seeds) {
        super(seeds);
    }

    public GetElements(final CloseableIterable<SEED_TYPE> seeds) {
        super(seeds);
    }

    public GetElements(final View view) {
        super(view);
    }

    public GetElements(final View view, final Iterable<SEED_TYPE> seeds) {
        super(view, seeds);
    }

    public GetElements(final View view, final CloseableIterable<SEED_TYPE> seeds) {
        super(view, seeds);
    }

    public GetElements(final GetOperation<SEED_TYPE, ?> operation) {
        super(operation);
    }

    public void setSeedMatching(final SeedMatchingType seedMatching) {
        super.setSeedMatching(seedMatching);
    }

    public abstract static class BaseBuilder<OP_TYPE extends GetElements<SEED_TYPE, ELEMENT_TYPE>,
            SEED_TYPE extends ElementSeed,
            ELEMENT_TYPE extends Element,
            CHILD_CLASS extends BaseBuilder<OP_TYPE, SEED_TYPE, ELEMENT_TYPE, ?>>
            extends AbstractGetOperation.BaseBuilder<OP_TYPE, SEED_TYPE, CloseableIterable<ELEMENT_TYPE>, CHILD_CLASS> {
        protected BaseBuilder(final OP_TYPE op) {
            super(op);
        }
    }

    public static final class Builder<OP_TYPE extends GetElements<SEED_TYPE, ELEMENT_TYPE>,
            SEED_TYPE extends ElementSeed,
            ELEMENT_TYPE extends Element>
            extends BaseBuilder<OP_TYPE, SEED_TYPE, ELEMENT_TYPE, Builder<OP_TYPE, SEED_TYPE, ELEMENT_TYPE>> {

        protected Builder(final OP_TYPE op) {
            super(op);
        }

        @Override
        protected Builder<OP_TYPE, SEED_TYPE, ELEMENT_TYPE> self() {
            return this;
        }
    }
}
