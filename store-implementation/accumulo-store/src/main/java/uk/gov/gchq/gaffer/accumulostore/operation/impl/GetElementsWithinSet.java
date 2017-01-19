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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.AbstractGetIterableElementsOperation;
import uk.gov.gchq.gaffer.operation.GetIterableElementsOperation;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import java.util.List;

/**
 * Retrieves {@link uk.gov.gchq.gaffer.data.element.Edge}s where both ends are in a given
 * set and/or {@link uk.gov.gchq.gaffer.data.element.Entity}s where the vertex is in the
 * set.
 **/
public class GetElementsWithinSet<ELEMENT_TYPE extends Element> extends AbstractGetIterableElementsOperation<EntitySeed, ELEMENT_TYPE> {

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

    public GetElementsWithinSet(final GetIterableElementsOperation<EntitySeed, ?> operation) {
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

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
    @JsonGetter(value = "seeds")
    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "if the iterable is null then the array should be null")
    @Override
    public EntitySeed[] getSeedArray() {
        final CloseableIterable<EntitySeed> input = getInput();
        if (null != input) {
            final List<EntitySeed> inputList = Lists.newArrayList(input);
            return inputList.toArray(new EntitySeed[inputList.size()]);
        }

        return null;
    }

    public abstract static class BaseBuilder<ELEMENT_TYPE extends Element, CHILD_CLASS extends BaseBuilder<ELEMENT_TYPE, ?>>
            extends AbstractGetIterableElementsOperation.BaseBuilder<GetElementsWithinSet<ELEMENT_TYPE>, EntitySeed, ELEMENT_TYPE, CHILD_CLASS> {
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
