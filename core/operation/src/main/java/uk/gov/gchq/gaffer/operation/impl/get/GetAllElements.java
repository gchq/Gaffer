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
import uk.gov.gchq.gaffer.operation.data.ElementSeed;

/**
 * Extends {@link GetElements}, but fetches all elements from the graph that are
 * compatible with the provided view.
 * There are also various flags to filter out the elements returned.
 *
 * @param <ELEMENT_TYPE> the element return type
 */
public class GetAllElements<ELEMENT_TYPE extends Element>
        extends GetElements<ElementSeed, ELEMENT_TYPE> {
    public GetAllElements() {
        super();
    }

    public GetAllElements(final View view) {
        super(view);
    }

    public GetAllElements(final GetAllElements<?> operation) {
        super(operation);
    }

    @Override
    public SeedMatchingType getSeedMatching() {
        return SeedMatchingType.EQUAL;
    }

    @Override
    public void setSeeds(final Iterable<ElementSeed> seeds) {
        if (null != seeds) {
            throw new IllegalArgumentException("This operation does not allow seeds to be set");
        }
    }

    @Override
    public void setSeeds(final CloseableIterable<ElementSeed> seeds) {
        if (null != seeds) {
            throw new IllegalArgumentException("This operation does not allow seeds to be set");
        }
    }

    @Override
    public void setInput(final CloseableIterable<ElementSeed> input) {
        if (null != input) {
            throw new IllegalArgumentException("This operation does not allow seeds to be set");
        }
    }

    @Override
    public CloseableIterable<ElementSeed> getSeeds() {
        return null;
    }

    @Override
    public CloseableIterable<ElementSeed> getInput() {
        return null;
    }

    @Override
    public IncludeIncomingOutgoingType getIncludeIncomingOutGoing() {
        return IncludeIncomingOutgoingType.OUTGOING;
    }

    @Override
    public void setIncludeIncomingOutGoing(final IncludeIncomingOutgoingType includeIncomingOutGoing) {
        if (!IncludeIncomingOutgoingType.OUTGOING.equals(includeIncomingOutGoing)) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " does not support any direction apart from outgoing edges");
        }
    }

    public abstract static class BaseBuilder<OP_TYPE extends GetAllElements<ELEMENT_TYPE>, ELEMENT_TYPE extends Element, CHILD_CLASS extends BaseBuilder<OP_TYPE, ELEMENT_TYPE, ?>>
            extends GetElements.BaseBuilder<OP_TYPE, ElementSeed, ELEMENT_TYPE, CHILD_CLASS> {
        public BaseBuilder(final OP_TYPE op) {
            super(op);
        }
    }

    public static final class Builder<ELEMENT_TYPE extends Element> extends BaseBuilder<GetAllElements<ELEMENT_TYPE>, ELEMENT_TYPE, Builder<ELEMENT_TYPE>> {
        public Builder() {
            super(new GetAllElements<ELEMENT_TYPE>());
        }

        @Override
        protected Builder<ELEMENT_TYPE> self() {
            return this;
        }
    }
}
