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

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.AbstractGetOperation;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;

/**
 * Retrieves {@link gaffer.data.element.Edge}s where both ends are in a given
 * set and/or {@link gaffer.data.element.Entity}s where the vertex is in the
 * set.
 **/
public class GetElementsWithinSet<ELEMENT_TYPE extends Element> extends AbstractGetOperation<EntitySeed, CloseableIterable<ELEMENT_TYPE>> {

    public GetElementsWithinSet() {
    }

    public GetElementsWithinSet(final Iterable<EntitySeed> seeds) {
        super(seeds);
    }

    public GetElementsWithinSet(final View view) {
        super(view);
    }

    public GetElementsWithinSet(final View view, final Iterable<EntitySeed> seeds) {
        super(view, seeds);
    }

    public GetElementsWithinSet(final GetOperation<EntitySeed, ?> operation) {
        super(operation);
    }

    @Override
    public IncludeIncomingOutgoingType getIncludeIncomingOutGoing() {
        return IncludeIncomingOutgoingType.OUTGOING;
    }

    @Override
    public void setIncludeIncomingOutGoing(final IncludeIncomingOutgoingType includeIncomingOutGoing) {
        if (!getIncludeIncomingOutGoing().equals(includeIncomingOutGoing)) {
            throw new IllegalArgumentException(
                    getClass().getSimpleName() + " you cannot change the IncludeIncomingOutgoingType on this operation");
        }
    }

    public abstract static class BaseBuilder<ELEMENT_TYPE extends Element, CHILD_CLASS extends BaseBuilder<ELEMENT_TYPE, ?>>
            extends AbstractGetOperation.BaseBuilder<GetElementsWithinSet<ELEMENT_TYPE>, EntitySeed, CloseableIterable<ELEMENT_TYPE>, CHILD_CLASS> {
        public BaseBuilder() {
            super(new GetElementsWithinSet<ELEMENT_TYPE>());
        }
    }

    public static final class Builder<ELEMENT_TYPE extends Element>
            extends BaseBuilder<ELEMENT_TYPE, Builder<ELEMENT_TYPE>> {

        @Override
        protected Builder self() {
            return this;
        }
    }
}
