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
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.operation.graph.AbstractSeededGraphGetIterable;

/**
 * This returns all data between the provided
 * {@link uk.gov.gchq.gaffer.data.element.id.ElementId}s.
 */
public class GetElementsInRanges<I_TYPE extends Pair<? extends ElementId>, E extends Element>
        extends AbstractSeededGraphGetIterable<I_TYPE, E> {
    public abstract static class BaseBuilder<I_TYPE extends Pair<? extends ElementId>,
            E extends Element,
            CHILD_CLASS extends BaseBuilder<I_TYPE, E, ?>>
            extends AbstractSeededGraphGetIterable.BaseBuilder<GetElementsInRanges<I_TYPE, E>, I_TYPE, E, CHILD_CLASS> {
        public BaseBuilder() {
            super(new GetElementsInRanges<>());
        }
    }

    public static final class Builder<I_TYPE extends Pair<? extends ElementId>,
            E extends Element>
            extends BaseBuilder<I_TYPE, E, Builder<I_TYPE, E>> {

        @Override
        protected Builder<I_TYPE, E> self() {
            return this;
        }
    }
}
