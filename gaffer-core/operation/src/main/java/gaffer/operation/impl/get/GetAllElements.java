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

import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.data.ElementSeed;

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
    public void setInput(final Iterable<ElementSeed> input) {
        if (null != input) {
            throw new IllegalArgumentException("This operation does not allow seeds to be set");
        }
    }

    @Override
    public Iterable<ElementSeed> getSeeds() {
        return null;
    }

    @Override
    public Iterable<ElementSeed> getInput() {
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

    public static class Builder<ELEMENT_TYPE extends Element>
            extends GetElements.Builder<GetAllElements<ELEMENT_TYPE>, ElementSeed, ELEMENT_TYPE> {
        public Builder() {
            this(new GetAllElements<ELEMENT_TYPE>());
        }

        public Builder(final GetAllElements<ELEMENT_TYPE> op) {
            super(op);
        }

        @Override
        public Builder<ELEMENT_TYPE> includeEntities(final boolean includeEntities) {
            super.includeEntities(includeEntities);
            return this;
        }

        @Override
        public Builder<ELEMENT_TYPE> includeEdges(final IncludeEdgeType includeEdgeType) {
            super.includeEdges(includeEdgeType);
            return this;
        }

        @Override
        public Builder<ELEMENT_TYPE> summarise(final boolean summarise) {
            super.summarise(summarise);
            return this;
        }

        @Override
        public Builder<ELEMENT_TYPE> deduplicate(final boolean deduplicate) {
            return (Builder<ELEMENT_TYPE>) super.deduplicate(deduplicate);
        }

        @Override
        public Builder<ELEMENT_TYPE> populateProperties(final boolean populateProperties) {
            super.populateProperties(populateProperties);
            return this;
        }

        @Override
        public Builder<ELEMENT_TYPE> view(final View view) {
            super.view(view);
            return this;
        }

        @Override
        public Builder<ELEMENT_TYPE> option(final String name, final String value) {
            super.option(name, value);
            return this;
        }
    }
}
