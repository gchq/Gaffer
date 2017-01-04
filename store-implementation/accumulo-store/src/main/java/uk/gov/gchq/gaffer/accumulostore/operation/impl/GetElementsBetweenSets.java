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
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;

/**
 * Given two sets of {@link uk.gov.gchq.gaffer.operation.data.EntitySeed}s, called A and B,
 * this retrieves all {@link uk.gov.gchq.gaffer.data.element.Edge}s where one end is in set
 * A and the other is in set B and also returns
 * {@link uk.gov.gchq.gaffer.data.element.Entity}s for
 * {@link uk.gov.gchq.gaffer.operation.data.EntitySeed}s in set A.
 */
public class GetElementsBetweenSets<ELEMENT_TYPE extends Element>
        extends AbstractAccumuloTwoSetSeededOperation<EntitySeed, ELEMENT_TYPE> {

    public GetElementsBetweenSets() {
    }

    public GetElementsBetweenSets(final Iterable<EntitySeed> seedsA, final Iterable<EntitySeed> seedsB) {
        super(seedsA, seedsB);
    }

    public GetElementsBetweenSets(final Iterable<EntitySeed> seedsA, final Iterable<EntitySeed> seedsB,
                                  final View view) {
        super(seedsA, seedsB, view);
    }

    public abstract static class BaseBuilder<ELEMENT_TYPE extends Element, CHILD_CLASS extends BaseBuilder<ELEMENT_TYPE, ?>>
            extends AbstractAccumuloTwoSetSeededOperation.BaseBuilder<GetElementsBetweenSets<ELEMENT_TYPE>, EntitySeed, ELEMENT_TYPE, CHILD_CLASS> {

        public BaseBuilder() {
            super(new GetElementsBetweenSets<ELEMENT_TYPE>());
        }
    }

    public static final class Builder<ELEMENT_TYPE extends Element>
            extends BaseBuilder<ELEMENT_TYPE, Builder<ELEMENT_TYPE>> {
        @Override
        protected Builder<ELEMENT_TYPE> self() {
            return this;
        }
    }
}
