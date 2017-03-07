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

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;

/**
 * A <code>Get</code> operation gets information out of gaffer and applies filtering
 * based on a {@link View}.
 *
 * @param <I>
 * @param <O>
 */
public interface Get<I, O> extends Operation<I, O> {
    /**
     * @return the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.View} for the operation.
     * @see uk.gov.gchq.gaffer.data.elementdefinition.view.View
     */
    View getView();

    /**
     * @param view the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.View} for the operation.
     * @see uk.gov.gchq.gaffer.data.elementdefinition.view.View
     */
    void setView(final View view);

    /**
     * @param element the {@link uk.gov.gchq.gaffer.data.element.Element} to be validated.
     * @return true if the {@link uk.gov.gchq.gaffer.data.element.Element} is valid. Otherwise false and a reason should be logged.
     * <p>
     * If the element class is known then validate(Entity) or validate(Edge) should be called instead to avoid
     * unnecessary use of <code>instanceof</code>.
     * @see Get#validate(Entity)
     * @see Get#validate(Edge)
     */
    boolean validate(final Element element);

    /**
     * @param edge the {@link uk.gov.gchq.gaffer.data.element.Edge} to be validated.
     * @return true if the {@link uk.gov.gchq.gaffer.data.element.Edge} is valid. Otherwise false and a reason should be logged.
     */
    boolean validate(final Edge edge);

    /**
     * @param entity the {@link uk.gov.gchq.gaffer.data.element.Entity} to be validated.
     * @return true if the {@link uk.gov.gchq.gaffer.data.element.Entity} is valid. Otherwise false and a reason should be logged.
     */
    boolean validate(final Entity entity);

    /**
     * Validates an element against the pre aggregation contained in the operation View.
     *
     * @param element the element to validate
     * @return true if the element is validate
     */
    boolean validatePreAggregationFilter(final Element element);

    /**
     * Validates an element against the post aggregation filters contained in the operation View.
     *
     * @param element the element to validate
     * @return true if the element is validate
     */
    boolean validatePostAggregationFilter(final Element element);

    /**
     * Validates an element against the post transform filters contained in the operation View.
     *
     * @param element the element to validate
     * @return true if the element is validate
     */
    boolean validatePostTransformFilter(final Element element);
}
