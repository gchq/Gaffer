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

package uk.gov.gchq.gaffer.operation.data;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;

/**
 * An {@code ElementSeed} contains the identifiers for an {@link uk.gov.gchq.gaffer.data.element.Entity} or
 * {@link uk.gov.gchq.gaffer.data.element.Edge}.
 * It is used as a mainly used as a seed for queries.
 */
public abstract class ElementSeed implements ElementId {
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    public static ElementSeed createSeed(final ElementId elementId) {
        if (elementId instanceof EntityId) {
            return createSeed((EntityId) elementId);
        }

        return createSeed((EdgeId) elementId);
    }

    public static EntitySeed createSeed(final EntityId entityId) {
        return new EntitySeed(entityId.getVertex());
    }

    public static EdgeSeed createSeed(final EdgeId edgeId) {
        return new EdgeSeed(edgeId.getSource(), edgeId.getDestination(), edgeId.getDirectedType());
    }
}
