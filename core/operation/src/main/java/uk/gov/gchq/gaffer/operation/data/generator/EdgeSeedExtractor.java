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

package uk.gov.gchq.gaffer.operation.data.generator;

import uk.gov.gchq.gaffer.data.Validator;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;

/**
 * Generates {@link uk.gov.gchq.gaffer.operation.data.EdgeSeed}s from and {@link uk.gov.gchq.gaffer.data.element.Edge}s.
 * The getElement() method is not supported as you cannot generate an <code>Element</code> from an
 * <code>EdgeSeed</code> - an {@link java.lang.UnsupportedOperationException} will be thrown if this is attempted.
 * getObject(Element) is not supported with {@link uk.gov.gchq.gaffer.data.element.Entity}s - an
 * {@link java.lang.IllegalArgumentException} will be thrown if this is attempted.
 */
public class EdgeSeedExtractor extends OneToOneElementGenerator<EdgeSeed> {

    public EdgeSeedExtractor() {
    }

    public EdgeSeedExtractor(final Validator<Element> elementValidator, final Validator<EdgeSeed> edgeSeedValidator, final boolean skipInvalid) {
        super(elementValidator, edgeSeedValidator, skipInvalid);
    }

    /**
     * This method is not supported and should not be used.
     *
     * @throws UnsupportedOperationException will always be thrown as this method should not be used.
     */
    @Override
    public Element getElement(final EdgeSeed item) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Cannot construct an element from an EdgeSeed");
    }

    /**
     * @param element the element to convert to {@link uk.gov.gchq.gaffer.operation.data.EdgeSeed}.
     * @return the {@link uk.gov.gchq.gaffer.operation.data.EdgeSeed} of the element
     * @throws IllegalArgumentException if the element is not an Edge.
     */
    @Override
    public EdgeSeed getObject(final Element element) throws IllegalArgumentException {
        try {
            return getObject(((Edge) element));
        } catch (final ClassCastException e) {
            throw new IllegalArgumentException("Cannot get an EdgeSeed from and Entity", e);
        }
    }

    public EdgeSeed getObject(final Edge element) {
        return new EdgeSeed(element.getSource(), element.getDestination(), element.isDirected());
    }
}
