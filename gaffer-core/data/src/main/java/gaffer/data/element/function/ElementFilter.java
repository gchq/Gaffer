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

package gaffer.data.element.function;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gaffer.data.element.Element;
import gaffer.data.element.ElementTuple;
import gaffer.function.FilterFunction;
import gaffer.function.processor.Filter;

/**
 * Element Filter - for filtering {@link gaffer.data.element.Element}s.
 * <p>
 * Use {@link gaffer.data.element.function.ElementAggregator.Builder} to build an ElementFilter.
 *
 * @see gaffer.data.element.function.ElementFilter.Builder
 * @see gaffer.function.processor.Filter
 */
public class ElementFilter extends Filter<String> {
    private final ElementTuple elementTuple = new ElementTuple();

    public boolean filter(final Element element) {
        elementTuple.setElement(element);
        return super.filter(elementTuple);
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "Uses super.cloneFunctions instead for better performance")
    @Override
    public ElementFilter clone() {
        final ElementFilter clone = new ElementFilter();
        clone.addFunctions(super.cloneFunctions());

        return clone;
    }

    /**
     * Builder for {@link ElementFilter}.
     */
    public static class Builder extends Filter.Builder<String> {
        public Builder() {
            this(new ElementFilter());
        }

        public Builder(final ElementFilter filter) {
            super(filter);
        }

        public Builder select(final String... selection) {
            return (Builder) super.select(selection);
        }

        public Builder execute(final FilterFunction function) {
            return (Builder) super.execute(function);
        }

        public ElementFilter build() {
            return (ElementFilter) super.build();
        }
    }
}
