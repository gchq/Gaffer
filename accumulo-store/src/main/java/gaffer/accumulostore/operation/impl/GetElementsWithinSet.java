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

package gaffer.accumulostore.operation.impl;

import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.AbstractGetOperation;
import gaffer.operation.GetOperation;
import gaffer.operation.data.EntitySeed;

/**
 * Retrieves {@link gaffer.data.element.Edge}s where both ends are in a given
 * set and/or {@link gaffer.data.element.Entity}s where the vertex is in the
 * set.
 *
 **/
public class GetElementsWithinSet<ELEMENT_TYPE extends Element> extends AbstractGetOperation<EntitySeed, ELEMENT_TYPE> {

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
        throw new IllegalArgumentException(
                getClass().getSimpleName() + " you cannot change the IncludeIncomingOutgoingType on this operation");
    }

    public static class Builder<OP_TYPE extends GetElementsWithinSet<ELEMENT_TYPE>, ELEMENT_TYPE extends Element>
            extends AbstractGetOperation.Builder<OP_TYPE, EntitySeed, ELEMENT_TYPE> {

        protected Builder(final OP_TYPE op) {
            super(op);
        }

    }

}
