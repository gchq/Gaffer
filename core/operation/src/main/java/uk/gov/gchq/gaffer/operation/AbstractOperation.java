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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractOperation<I, O> implements Operation<I, O> {
    /**
     * The operation view. This allows filters and transformations to be applied to the graph.
     */
    private View view;

    /**
     * The input for the operation.
     */
    private I input;

    private Map<String, String> options = new HashMap<>();
    private TypeReference<?> outputTypeReference = createOutputTypeReference();

    @Override
    public O castToOutputType(final Object result) {
        return (O) result;
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
        final ViewElementDefinition elementDef = view.getElement(element.getGroup());
        return null != elementDef && (null == elementDef.getPreAggregationFilter() || elementDef.getPreAggregationFilter().filter(element));
    }

    @Override
    public boolean validatePostAggregationFilter(final Element element) {
        final ViewElementDefinition elementDef = view.getElement(element.getGroup());
        return null != elementDef && (null == elementDef.getPostAggregationFilter() || elementDef.getPostAggregationFilter().filter(element));
    }

    @Override
    public boolean validatePostTransformFilter(final Element element) {
        final ViewElementDefinition elementDef = view.getElement(element.getGroup());
        return null != elementDef && (null == elementDef.getPostTransformFilter() || elementDef.getPostTransformFilter().filter(element));
    }


    @Override
    public I getInput() {
        return input;
    }

    @Override
    public void setInput(final I input) {
        this.input = input;
    }

    @Override
    public View getView() {
        return view;
    }

    @Override
    public void setView(final View view) {
        this.view = view;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public void addOption(final String name, final String value) {
        this.options.put(name, value);
    }

    @Override
    public String getOption(final String name) {
        return this.options.get(name);
    }

    @JsonGetter("options")
    Map<String, String> getJsonOptions() {
        return options.isEmpty() ? null : options;
    }

    @JsonIgnore
    @Override
    public TypeReference<O> getOutputTypeReference() {
        return (TypeReference<O>) outputTypeReference;
    }

    @Override
    public void setOutputTypeReference(final TypeReference<?> outputTypeReference) {
        this.outputTypeReference = outputTypeReference;
    }

    protected abstract TypeReference createOutputTypeReference();

    public abstract static class BaseBuilder<OP_TYPE extends AbstractOperation<I, O>,
            I,
            O,
            CHILD_CLASS extends BaseBuilder<OP_TYPE, I, O, ?>> {
        protected OP_TYPE op;

        protected BaseBuilder(final OP_TYPE op) {
            this.op = op;
        }

        /**
         * Builds the operation and returns it.
         *
         * @return the built operation.
         */
        public OP_TYPE build() {
            return op;
        }

        /**
         * @param input the input to set on the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Operation#setInput(Object)
         */
        public CHILD_CLASS input(final I input) {
            op.setInput(input);
            return self();
        }

        /**
         * @param view the view to set on the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Operation#setView(View)
         */
        public CHILD_CLASS view(final View view) {
            op.setView(view);
            return self();
        }

        /**
         * @param name  the name of the option to add
         * @param value the value of the option to add
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Operation#addOption(String, String)
         */
        public CHILD_CLASS option(final String name, final String value) {
            op.addOption(name, value);
            return self();
        }

        public CHILD_CLASS outputType(final TypeReference<?> typeReference) {
            op.setOutputTypeReference(typeReference);
            return self();
        }

        public CHILD_CLASS copy(final OP_TYPE opToCopy) {
            op.setView(opToCopy.getView());
            op.setInput(opToCopy.getInput());
            op.setOptions(new HashMap<>(opToCopy.getOptions()));
            return self();
        }

        protected abstract CHILD_CLASS self();

        protected OP_TYPE getOp() {
            return op;
        }
    }
}
