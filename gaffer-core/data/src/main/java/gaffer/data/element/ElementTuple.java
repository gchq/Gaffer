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

package gaffer.data.element;


import gaffer.function.Tuple;

/**
 * An <code>ElementTuple</code> implements {@link gaffer.function.Tuple} wrapping an
 * {@link gaffer.data.element.Element} and providing a getter and setter for the element's identifiers and properties.
 * This class allows Elements to be used with the function module whilst minimising dependencies.
 */
public class ElementTuple implements Tuple<ElementComponentKey> {
    private Element element;

    public ElementTuple() {
    }

    public ElementTuple(final Element element) {
        this.element = element;
    }

    public Element getElement() {
        return element;
    }

    public void setElement(final Element element) {
        this.element = element;
    }

    @Override
    public Object get(final ElementComponentKey reference) {
        if (reference.isId()) {
            return element.getIdentifier(reference.getIdentifierType());
        } else {
            return element.getProperty(reference.getPropertyName());
        }
    }

    @Override
    public void put(final ElementComponentKey reference, final Object value) {
        if (reference.isId()) {
            element.putIdentifier(reference.getIdentifierType(), value);
        } else {
            element.putProperty(reference.getPropertyName(), value);
        }
    }

    @Override
    public String toString() {
        return "ElementTuple{"
                + "element=" + element
                + '}';
    }
}
