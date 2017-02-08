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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.data.Validator;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;

/**
 * Generates {@link uk.gov.gchq.gaffer.operation.data.ElementSeed}s from and {@link uk.gov.gchq.gaffer.data.element.Element}s.
 * The getElement() method is not supported as you cannot generate an <code>Element</code> from an
 * <code>EdgeSeed</code> - an {@link java.lang.UnsupportedOperationException} will be thrown if this is attempted.
 * getObject(Element) is not supported with {@link uk.gov.gchq.gaffer.data.element.Entity}s - an
 * {@link java.lang.IllegalArgumentException} will be thrown if this is attempted.
 */
public class EntitySeedExtractor extends OneToOneElementGenerator<EntitySeed> {
    private IdentifierType edgeIdentifierToExtract;

    public EntitySeedExtractor() {
        this(IdentifierType.DESTINATION);
    }

    public EntitySeedExtractor(final IdentifierType edgeIdentifierToExtract) {
        super();
        this.edgeIdentifierToExtract = edgeIdentifierToExtract;
    }

    public EntitySeedExtractor(final Validator<Element> elementValidator, final Validator<EntitySeed> objectValidator,
                               final boolean skipInvalid, final IdentifierType edgeIdentifierToExtract) {
        super(elementValidator, objectValidator, skipInvalid);
        this.edgeIdentifierToExtract = edgeIdentifierToExtract;
    }

    public IdentifierType getEdgeIdentifierToExtract() {
        return edgeIdentifierToExtract;
    }

    public void setEdgeIdentifierToExtract(final IdentifierType edgeIdentifierToExtract) {
        this.edgeIdentifierToExtract = edgeIdentifierToExtract;
    }

    /**
     * This method is not supported and should not be used.
     *
     * @throws UnsupportedOperationException will always be thrown as this method should not be used.
     */
    @Override
    public Element getElement(final EntitySeed seed) {
        throw new UnsupportedOperationException("Cannot construct an element from an identifier");
    }

    /**
     * @param element the element to convert to {@link uk.gov.gchq.gaffer.operation.data.EntitySeed}.
     * @return the {@link uk.gov.gchq.gaffer.operation.data.EntitySeed} extracted from the element
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    @Override
    public EntitySeed getObject(final Element element) {
        final Object identifier;
        if (element instanceof Entity) {
            identifier = ((Entity) element).getVertex();
        } else if (IdentifierType.SOURCE == edgeIdentifierToExtract) {
            identifier = ((Edge) element).getSource();
        } else if (IdentifierType.DESTINATION == edgeIdentifierToExtract) {
            identifier = ((Edge) element).getDestination();
        } else {
            throw new IllegalArgumentException("Cannot get an EntitySeed from an Edge when IdentifierType is " + edgeIdentifierToExtract);
        }

        return new EntitySeed(identifier);
    }
}
