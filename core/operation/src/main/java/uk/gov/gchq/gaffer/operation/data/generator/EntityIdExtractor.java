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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.generator.OneToOneObjectGenerator;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.koryphe.Since;

/**
 * Generates {@link uk.gov.gchq.gaffer.data.element.id.ElementId}s from and {@link uk.gov.gchq.gaffer.data.element.Element}s.
 * getObject(Element) is not supported with {@link uk.gov.gchq.gaffer.data.element.Entity}s - an
 * {@link java.lang.IllegalArgumentException} will be thrown if this is attempted.
 */
@Since("1.0.0")
public class EntityIdExtractor implements OneToOneObjectGenerator<EntityId> {
    private IdentifierType edgeIdentifierToExtract;

    public EntityIdExtractor() {
        this(IdentifierType.DESTINATION);
    }

    public EntityIdExtractor(final IdentifierType edgeIdentifierToExtract) {
        super();
        this.edgeIdentifierToExtract = edgeIdentifierToExtract;
    }

    public IdentifierType getEdgeIdentifierToExtract() {
        return edgeIdentifierToExtract;
    }

    public void setEdgeIdentifierToExtract(final IdentifierType edgeIdentifierToExtract) {
        this.edgeIdentifierToExtract = edgeIdentifierToExtract;
    }

    /**
     * @param element the element to convert to {@link uk.gov.gchq.gaffer.data.element.id.EntityId}.
     * @return the {@link uk.gov.gchq.gaffer.data.element.id.EntityId} extracted from the element
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    @Override
    public EntityId _apply(final Element element) {
        final Object identifier;
        if (element instanceof Entity) {
            identifier = ((Entity) element).getVertex();
        } else if (IdentifierType.SOURCE == edgeIdentifierToExtract) {
            identifier = ((Edge) element).getSource();
        } else if (IdentifierType.DESTINATION == edgeIdentifierToExtract) {
            identifier = ((Edge) element).getDestination();
        } else if (IdentifierType.MATCHED_VERTEX == edgeIdentifierToExtract) {
            identifier = ((Edge) element).getMatchedVertexValue();
        } else if (IdentifierType.ADJACENT_MATCHED_VERTEX == edgeIdentifierToExtract) {
            identifier = ((Edge) element).getAdjacentMatchedVertexValue();
        } else {
            throw new IllegalArgumentException("Cannot get an EntityId from an Edge when IdentifierType is " + edgeIdentifierToExtract);
        }

        return new EntitySeed(identifier);
    }
}
