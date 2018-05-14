/*
 * Copyright 2016-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.generator.OneToOneObjectGenerator;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

/**
 * Generates {@link uk.gov.gchq.gaffer.data.element.id.EdgeId}s from and {@link uk.gov.gchq.gaffer.data.element.Edge}s.
 * getObject(Element) is not supported with {@link uk.gov.gchq.gaffer.data.element.Entity}s - an
 * {@link java.lang.IllegalArgumentException} will be thrown if this is attempted.
 */
@Since("1.0.0")
@Summary("Generates EdgeIds from an Edge")
public class EdgeIdExtractor implements OneToOneObjectGenerator<EdgeId> {
    /**
     * @param element the element to convert to {@link uk.gov.gchq.gaffer.data.element.id.EdgeId}.
     * @return the {@link uk.gov.gchq.gaffer.data.element.id.EdgeId} of the element
     * @throws IllegalArgumentException if the element is not an Edge.
     */
    @Override
    public EdgeId _apply(final Element element) {
        try {
            return _apply((Edge) element);
        } catch (final ClassCastException e) {
            throw new IllegalArgumentException("Cannot get an EdgeId from and Entity", e);
        }
    }

    public EdgeId _apply(final Edge element) {
        return new EdgeSeed(element.getSource(), element.getDestination(), element.isDirected());
    }
}
