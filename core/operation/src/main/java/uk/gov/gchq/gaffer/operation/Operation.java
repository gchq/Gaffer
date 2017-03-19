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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import java.util.Map;

/**
 * An <code>Operation</code> defines an operation to be processed on a graph.
 * All operations must to implement this class.
 * Operations should be written to be as generic as possible to allow them to be applied to different graph/stores.
 * NOTE - operations should not contain the operation logic. The logic should be separated out into a operation handler.
 * This will allow you to execute the same operation on different stores with different handlers.
 * <p>
 * This interface enforces all operations have the ability to supply a {@link View}, chain operations together
 * and provide an input for the operation.
 * <p>
 * Operations must be JSON serialisable in order to make REST API calls.
 *
 * @param <INPUT>  the input type of the operation. This must be JSON serialisable.
 * @param <OUTPUT> the output type of the operation. This must be JSON serialisable.
 */
public interface Operation<INPUT, OUTPUT> {
    /**
     * @param element the {@link uk.gov.gchq.gaffer.data.element.Element} to be validated.
     * @return true if the {@link uk.gov.gchq.gaffer.data.element.Element} is valid. Otherwise false and a reason should be logged.
     * <p>
     * If the element class is known then validate(Entity) or validate(Edge) should be called instead to avoid
     * unnecessary use of <code>instanceof</code>.
     * @see Operation#validate(Entity)
     * @see Operation#validate(Edge)
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

    OUTPUT castToOutputType(final Object result);

    @JsonIgnore
    TypeReference<OUTPUT> getOutputTypeReference();

    void setOutputTypeReference(final TypeReference<?> outputTypeReference);

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

    /**
     * @return the operation input.
     */
    INPUT getInput();

    /**
     * @param input the operation input to be set.
     *              This can happen automatically from a previous operation if this operation is used in an
     *              {@link OperationChain}.
     */
    void setInput(final INPUT input);

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
     * @return the operation options. This may contain store specific options such as authorisation strings or and
     * other properties required for the operation to be executed. Note these options will probably not be interpreted
     * in the same way by every store implementation.
     */
    Map<String, String> getOptions();

    /**
     * @param options the operation options. This may contain store specific options such as authorisation strings or and
     *                other properties required for the operation to be executed. Note these options will probably not be interpreted
     *                in the same way by every store implementation.
     */
    void setOptions(final Map<String, String> options);

    /**
     * Adds an operation option. This may contain store specific options such as authorisation strings or and
     * other properties required for the operation to be executed. Note these options will probably not be interpreted
     * in the same way by every store implementation.
     *
     * @param name  the name of the option
     * @param value the value of the option
     */
    void addOption(final String name, final String value);

    /**
     * Gets an operation option by its given name.
     *
     * @param name the name of the option
     * @return the value of the option
     */
    String getOption(final String name);
}

