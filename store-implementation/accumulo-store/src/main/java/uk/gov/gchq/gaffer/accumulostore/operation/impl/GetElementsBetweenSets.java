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

import uk.gov.gchq.gaffer.accumulostore.operation.AbstractAccumuloTwoSetSeededOperation;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;

/**
 * Given two sets of {@link uk.gov.gchq.gaffer.data.element.id.EntityId}s, called A and B,
 * this retrieves all {@link uk.gov.gchq.gaffer.data.element.Edge}s where one end is in set
 * A and the other is in set B and also returns
 * {@link uk.gov.gchq.gaffer.data.element.Entity}s for
 * {@link uk.gov.gchq.gaffer.data.element.id.EntityId}s in set A.
 */
public class GetElementsBetweenSets<E extends Element>
        extends AbstractAccumuloTwoSetSeededOperation<EntityId, E> {
    public abstract static class BaseBuilder<E extends Element, CHILD_CLASS extends BaseBuilder<E, ?>>
            extends AbstractAccumuloTwoSetSeededOperation.BaseBuilder<GetElementsBetweenSets<E>, EntityId, E, CHILD_CLASS> {

        public BaseBuilder() {
            super(new GetElementsBetweenSets<E>());
        }
    }

    public static final class Builder<E extends Element>
            extends BaseBuilder<E, Builder<E>> {
        @Override
        protected Builder<E> self() {
            return this;
        }
    }
}
