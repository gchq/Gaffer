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
package gaffer.operation.simple;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import gaffer.commonutil.Pair;
import gaffer.data.element.Element;
import gaffer.operation.AbstractOperation;
import gaffer.operation.VoidOutput;
import java.util.List;

/**
 * An <code>UpdateStore</code> operation will update elements given an
 * {@link Iterable} of {@link gaffer.commonutil.Pair}s of {@link gaffer.data.element.Element}s.
 * The first element in the pair is used to look up the existing element - it
 * does not have to be fully populated, just populated with sufficient
 * information to uniquely identify a stored element.
 * The second element in the pair is used to update the element.
 * <p>
 * Store handlers are responsible for validating the updated elements.
 *
 * @see UpdateElements.Builder
 */
public class UpdateElements extends AbstractOperation<Iterable<Pair<Element>>, Void> implements VoidOutput<Iterable<Pair<Element>>> {
    private boolean validate;
    private boolean skipInvalidElements;
    private boolean overrideProperties;

    public Iterable<Pair<Element>> getElementPairs() {
        return getInput();
    }

    public void setElementPairs(final Iterable<Pair<Element>> elements) {
        setInput(elements);
    }

    @JsonIgnore
    @Override
    public Iterable<Pair<Element>> getInput() {
        return super.getInput();
    }

    @JsonProperty(value = "elementPairs")
    List<Pair<Element>> getElementPairList() {
        final Iterable<Pair<Element>> input = getInput();
        return null != input ? Lists.newArrayList(input) : null;
    }

    @JsonProperty(value = "elementPairs")
    void setElementList(final List<Pair<Element>> elements) {
        setInput(elements);
    }

    public boolean isSkipInvalidElements() {
        return skipInvalidElements;
    }

    public boolean isValidate() {
        return validate;
    }

    public void setSkipInvalidElements(final boolean skipInvalidElements) {
        this.skipInvalidElements = skipInvalidElements;
    }

    public void setValidate(final boolean validate) {
        this.validate = validate;
    }

    public boolean isOverrideProperties() {
        return overrideProperties;
    }

    public void setOverrideProperties(final boolean overrideProperties) {
        this.overrideProperties = overrideProperties;
    }

    public static class Builder extends AbstractOperation.Builder<UpdateElements, Iterable<Pair<Element>>, Void> {
        public Builder() {
            super(new UpdateElements());
        }

        public Builder elementPairs(final Iterable<Pair<Element>> elements) {
            super.input(elements);
            return this;
        }

        public Builder skipInvalidElements(final boolean skipInvalidElements) {
            getOp().setSkipInvalidElements(skipInvalidElements);
            return this;
        }

        public Builder validate(final boolean validate) {
            getOp().setValidate(validate);
            return this;
        }

        public Builder overrideProperties(final boolean overrideProperties) {
            getOp().setOverrideProperties(overrideProperties);
            return this;
        }

        @Override
        public Builder option(final String name, final String value) {
            return (Builder) super.option(name, value);
        }
    }
}
