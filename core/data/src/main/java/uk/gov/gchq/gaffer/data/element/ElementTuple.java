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

package uk.gov.gchq.gaffer.data.element;


import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.koryphe.tuple.Tuple;

/**
 * An <code>ElementTuple</code> implements {@link Tuple} wrapping an
 * {@link Element} and providing a getter and setter for the element's identifiers and properties.
 * This class allows Elements to be used with the function module whilst minimising dependencies.
 */
public class ElementTuple implements Tuple<String> {
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
    public Object get(final String reference) {
        final IdentifierType idType = IdentifierType.fromName(reference);
        if (null == idType) {
            return element.getProperty(reference);
        }

        return element.getIdentifier(idType);
    }

    @Override
    public Iterable<Object> values() {
        throw new UnsupportedOperationException("Calling values() is not supported for ElementTuples");
    }

    @Override
    public void put(final String reference, final Object value) {
        final IdentifierType idType = IdentifierType.fromName(reference);

        if (null == idType) {
            element.putProperty(reference, value);
        } else {
            element.putIdentifier(idType, value);
        }
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        final ElementTuple objects = (ElementTuple) obj;

        return new EqualsBuilder()
                .append(element, objects.element)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(61, 5)
                .append(element)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("element", element)
                .toString();
    }
}
