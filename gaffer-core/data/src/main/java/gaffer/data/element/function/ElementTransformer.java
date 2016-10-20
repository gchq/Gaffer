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
import gaffer.function.TransformFunction;
import gaffer.function.processor.Transformer;

/**
 * Element Transformer - for transforming {@link gaffer.data.element.Element}s.
 * <p>
 * Use {@link gaffer.data.element.function.ElementTransformer.Builder} to build an ElementTransformer.
 *
 * @see gaffer.data.element.function.ElementTransformer.Builder
 * @see gaffer.function.processor.Transformer
 */
public class ElementTransformer extends Transformer<String> {
    private final ElementTuple elementTuple = new ElementTuple();

    public void transform(final Element element) {
        elementTuple.setElement(element);
        super.transform(elementTuple);
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "Uses super.cloneFunctions instead for better performance")
    @Override
    public ElementTransformer clone() {
        final ElementTransformer clone = new ElementTransformer();
        clone.addFunctions(super.cloneFunctions());

        return clone;
    }

    /**
     * Builder for {@link ElementTransformer}.
     */
    public static class Builder extends Transformer.Builder<String> {
        public Builder() {
            this(new ElementTransformer());
        }

        public Builder(final ElementTransformer transformer) {
            super(transformer);
        }

        public Builder select(final String... selection) {
            return (Builder) super.select(selection);
        }

        public Builder project(final String... projection) {
            return (Builder) super.project(projection);
        }

        public Builder execute(final TransformFunction function) {
            return (Builder) super.execute(function);
        }

        public ElementTransformer build() {
            return (ElementTransformer) super.build();
        }
    }
}
