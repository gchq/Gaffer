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

package uk.gov.gchq.gaffer.operation.graph;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.operation.AbstractGet;

public abstract class AbstractGraphGet<I, O>
        extends AbstractGet<I, O> implements GraphGet<I, O> {
    private DirectedType directedType = DirectedType.BOTH;

    @Override
    public void setDirectedType(final DirectedType directedType) {
        this.directedType = directedType;
    }

    @Override
    public DirectedType getDirectedType() {
        return directedType;
    }

    @Override
    public boolean validate(final Edge edge) {
        return null != edge && validateFlags(edge) && super.validate(edge);
    }

    public boolean validateFlags(final Edge edge) {
        return DirectedType.BOTH == getDirectedType()
                || (DirectedType.DIRECTED == getDirectedType() && edge.isDirected())
                || (DirectedType.UNDIRECTED == getDirectedType() && !edge.isDirected());
    }

    public abstract static class BaseBuilder<
            OP_TYPE extends AbstractGraphGet<I, O>,
            I,
            O,
            CHILD_CLASS extends BaseBuilder<OP_TYPE, I, O, ?>
            >
            extends AbstractGet.BaseBuilder<OP_TYPE, I, O, CHILD_CLASS> {

        protected BaseBuilder(final OP_TYPE op) {
            super(op);
        }

        /**
         * @param directedType sets the directedType option on the operation.
         * @return this Builder
         */
        public CHILD_CLASS directedType(final DirectedType directedType) {
            op.setDirectedType(directedType);
            return self();
        }
    }
}
