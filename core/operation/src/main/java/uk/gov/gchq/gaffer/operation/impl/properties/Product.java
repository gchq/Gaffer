/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.properties;

import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

/**
 * A <code>Product</code> operation can be used to calculate the product of all occurances of a property found in an {@link java.lang.Iterable}.
 *
 * @see uk.gov.gchq.gaffer.operation.impl.properties.Product
 */
public class Product implements
        Operation,
        InputOutput<Iterable<? extends Element>, Long>,
        MultiInput<Element> {

    private Iterable<? extends Element> input;
    private String propertyName;

    @Override
    public Iterable<? extends Element> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends Element> input) {
        this.input = input;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(final String propertyName) {
        this.propertyName = propertyName;
    }

    @Override
    public TypeReference<Long> getOutputTypeReference() {
        return new TypeReferenceImpl.Long();
    }

    public static final class Builder
            extends BaseBuilder<Product, Product.Builder>
            implements InputOutput.Builder<Product, Iterable<? extends Element>, Long, Product.Builder>,
            MultiInput.Builder<Product, Element, Product.Builder> {
        public Builder() {
            super(new Product());
        }

        public Builder propertyName(final String propertyName) {
            _getOp().setPropertyName(propertyName);
            return _self();
        }
    }
}
