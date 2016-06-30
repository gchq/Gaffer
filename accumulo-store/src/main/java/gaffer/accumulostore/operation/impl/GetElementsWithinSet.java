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
 **/
public class GetElementsWithinSet<ELEMENT_TYPE extends Element> extends AbstractGetOperation<EntitySeed, ELEMENT_TYPE> {

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
        throw new IllegalArgumentException(
                getClass().getSimpleName() + " you cannot change the IncludeIncomingOutgoingType on this operation");
    }

    public static class Builder<ELEMENT_TYPE extends Element>
            extends AbstractGetOperation.Builder<GetElementsWithinSet<ELEMENT_TYPE>, EntitySeed, ELEMENT_TYPE> {

        public Builder() {
            super(new GetElementsWithinSet<ELEMENT_TYPE>());
        }

        @Override
        public Builder<ELEMENT_TYPE> summarise(final boolean summarise) {
            return (Builder<ELEMENT_TYPE>) super.summarise(summarise);
        }

        @Override
        public Builder<ELEMENT_TYPE> deduplicate(final boolean deduplicate) {
            return (Builder<ELEMENT_TYPE>) super.deduplicate(deduplicate);
        }

        @Override
        public Builder<ELEMENT_TYPE> populateProperties(final boolean populateProperties) {
            return (Builder<ELEMENT_TYPE>) super.populateProperties(populateProperties);
        }

        @Override
        public Builder<ELEMENT_TYPE> view(final View view) {
            return (Builder<ELEMENT_TYPE>) super.view(view);
        }

        @Override
        public Builder<ELEMENT_TYPE> option(final String name, final String value) {
            return (Builder<ELEMENT_TYPE>) super.option(name, value);
        }

        @Override
        public Builder<ELEMENT_TYPE> seeds(final Iterable<EntitySeed> newSeeds) {
            return (Builder<ELEMENT_TYPE>) super.seeds(newSeeds);
        }

        @Override
        public Builder<ELEMENT_TYPE> addSeed(final EntitySeed seed) {
            return (Builder<ELEMENT_TYPE>) super.addSeed(seed);
        }

        @Override
        public Builder<ELEMENT_TYPE> includeEdges(final IncludeEdgeType includeEdgeType) {
            return (Builder<ELEMENT_TYPE>) super.includeEdges(includeEdgeType);
        }

        @Override
        public Builder<ELEMENT_TYPE> includeEntities(final boolean includeEntities) {
            return (Builder<ELEMENT_TYPE>) super.includeEntities(includeEntities);
        }

    }
}
