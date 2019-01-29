/*
 * Copyright 2018-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.impl.loader.schemas;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;

import java.util.Map;

/**
 * The {@code SchemaLoader} implementations are used alongside the {@link uk.gov.gchq.gaffer.integration.impl.loader.AbstractLoaderIT}
 * to create a valid set of elements for a given {@link uk.gov.gchq.gaffer.store.schema.Schema}.
 */
public interface SchemaLoader {

    // Identifier prefixes
    String SOURCE = "1-Source";
    String DEST = "2-Dest";
    String SOURCE_DIR = "1-SourceDir";
    String DEST_DIR = "2-DestDir";
    String A = "A";
    String B = "B";
    String C = "C";
    String D = "D";
    String[] VERTEX_PREFIXES = new String[]{A, B, C, D};

    Map<EdgeId, Edge> createEdges();

    Map<EntityId, Entity> createEntities();

    default void addToMap(final Edge element, final Map<EdgeId, Edge> edges) {
        edges.put(ElementSeed.createSeed(element), element);
    }

    default void addToMap(final Entity element, final Map<EntityId, Entity> entities) {
        entities.put(ElementSeed.createSeed(element), element);
    }

}
