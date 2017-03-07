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
import uk.gov.gchq.gaffer.operation.VoidInput;
import uk.gov.gchq.gaffer.operation.graph.AbstractGraphGetIterable;

/**
 * Extends {@link GetElements}, but fetches all elements from the graph that are
 * compatible with the provided view.
 * There are also various flags to filter out the elements returned.
 *
 * @param <E> the element return type
 */
public class GetAllElements<E extends Element>
        extends AbstractGraphGetIterable<Void, E>
        implements VoidInput<CloseableIterable<E>> {
    @Override
    public Void getInput() {
        return null;
    }

    public abstract static class BaseBuilder<OP_TYPE extends GetAllElements<E>, E extends Element, CHILD_CLASS extends BaseBuilder<OP_TYPE, E, ?>>
            extends AbstractGraphGetIterable.BaseBuilder<OP_TYPE, Void, E, CHILD_CLASS> {
        public BaseBuilder(final OP_TYPE op) {
            super(op);
        }
    }

    public static final class Builder<E extends Element> extends BaseBuilder<GetAllElements<E>, E, Builder<E>> {
        public Builder() {
            super(new GetAllElements<>());
        }

        @Override
        protected Builder<E> self() {
            return this;
        }
    }
}
