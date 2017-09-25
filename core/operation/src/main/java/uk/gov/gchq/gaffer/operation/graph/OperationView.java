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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.Operation;

/**
 * An {@code OperationView} operation contains a {@link View} and can carry out
 * additional validation based on the view contents.
 */
public interface OperationView {
    /**
     * @return the {@link View} for the operation.
     * @see View
     */
    View getView();

    /**
     * @param view the {@link View} for the operation.
     * @see View
     */
    void setView(final View view);

    /**
     * @param element the {@link Element} to be validated.
     * @return true if the {@link Element} is valid. Otherwise false and a reason should be logged.
     * <p>
     * If the element class is known then validate(Entity) or validate(Edge) should be called instead to avoid
     * unnecessary use of {@code instanceof}.
     * @see OperationView#validate(Entity)
     * @see OperationView#validate(Edge)
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Edge it must be an Entity")
    default boolean validate(final Element element) {
        return null != element
                && element instanceof Edge ? validate((Edge) element) : validate((Entity) element);
    }

    /**
     * @param edge the {@link Edge} to be validated.
     * @return true if the {@link Edge} is valid. Otherwise false and a reason should be logged.
     */
    default boolean validate(final Edge edge) {
        return null != edge
                && validatePreAggregationFilter(edge)
                && validatePostAggregationFilter(edge)
                && validatePostTransformFilter(edge);
    }

    /**
     * @param entity the {@link Entity} to be validated.
     * @return true if the {@link Entity} is valid. Otherwise false and a reason should be logged.
     */
    default boolean validate(final Entity entity) {
        return null != entity
                && validatePreAggregationFilter(entity)
                && validatePostAggregationFilter(entity)
                && validatePostTransformFilter(entity);
    }

    /**
     * Validates an element against the pre aggregation contained in the operation View.
     *
     * @param element the element to validate
     * @return true if the element is validate
     */
    default boolean validatePreAggregationFilter(final Element element) {
        if (null == getView()) {
            return false;
        }
        final ViewElementDefinition elementDef = getView().getElement(element.getGroup());
        return null != elementDef && (null == elementDef.getPreAggregationFilter() || elementDef.getPreAggregationFilter().test(element));
    }

    /**
     * Validates an element against the post aggregation filters contained in the operation View.
     *
     * @param element the element to validate
     * @return true if the element is validate
     */
    default boolean validatePostAggregationFilter(final Element element) {
        if (null == getView()) {
            return false;
        }
        final ViewElementDefinition elementDef = getView().getElement(element.getGroup());
        return null != elementDef && (null == elementDef.getPostAggregationFilter() || elementDef.getPostAggregationFilter().test(element));
    }

    /**
     * Validates an element against the post transform filters contained in the operation View.
     *
     * @param element the element to validate
     * @return true if the element is validate
     */
    default boolean validatePostTransformFilter(final Element element) {
        if (null == getView()) {
            return false;
        }
        final ViewElementDefinition elementDef = getView().getElement(element.getGroup());
        return null != elementDef && (null == elementDef.getPostTransformFilter() || elementDef.getPostTransformFilter().test(element));
    }

    interface Builder<OP extends OperationView, B extends Builder<OP, ?>> extends Operation.Builder<OP, B> {
        default B view(final View view) {
            _getOp().setView(view);
            return _self();
        }
    }
}
