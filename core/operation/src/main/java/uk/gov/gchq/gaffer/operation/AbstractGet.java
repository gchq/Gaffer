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

package uk.gov.gchq.gaffer.operation;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;

public abstract class AbstractGet<I, O>
        extends AbstractOperation<I, O> implements Get<I, O> {
    /**
     * The operation view. This allows filters and transformations to be applied to the graph.
     */
    private View view;

    @Override
    public View getView() {
        return view;
    }

    @Override
    public void setView(final View view) {
        this.view = view;
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Edge it must be an Entity")
    @Override
    public boolean validate(final Element element) {
        return null != element
                && element instanceof Edge ? validate((Edge) element) : validate((Entity) element);
    }

    @Override
    public boolean validate(final Edge edge) {
        return null != edge && validatePreAggregationFilter(edge) && validatePostAggregationFilter(edge) && validatePostTransformFilter(edge);
    }

    @Override
    public boolean validate(final Entity entity) {
        return null != entity && validatePreAggregationFilter(entity) && validatePostAggregationFilter(entity) && validatePostTransformFilter(entity);
    }

    @Override
    public boolean validatePreAggregationFilter(final Element element) {
        if (null == view) {
            return false;
        }
        final ViewElementDefinition elementDef = view.getElement(element.getGroup());
        return null != elementDef && (null == elementDef.getPreAggregationFilter() || elementDef.getPreAggregationFilter().filter(element));
    }

    @Override
    public boolean validatePostAggregationFilter(final Element element) {
        if (null == view) {
            return false;
        }
        final ViewElementDefinition elementDef = view.getElement(element.getGroup());
        return null != elementDef && (null == elementDef.getPostAggregationFilter() || elementDef.getPostAggregationFilter().filter(element));
    }

    @Override
    public boolean validatePostTransformFilter(final Element element) {
        if (null == view) {
            return false;
        }
        final ViewElementDefinition elementDef = view.getElement(element.getGroup());
        return null != elementDef && (null == elementDef.getPostTransformFilter() || elementDef.getPostTransformFilter().filter(element));
    }

    public abstract static class BaseBuilder<
            OP_TYPE extends AbstractGet<I, O>,
            I,
            O,
            CHILD_CLASS extends BaseBuilder<OP_TYPE, I, O, ?>
            >
            extends AbstractOperation.BaseBuilder<OP_TYPE, I, O, CHILD_CLASS> {

        protected BaseBuilder(final OP_TYPE op) {
            super(op);
        }

        /**
         * @param view the view to set on the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Get#setView(View)
         */
        public CHILD_CLASS view(final View view) {
            op.setView(view);
            return self();
        }
    }
}
