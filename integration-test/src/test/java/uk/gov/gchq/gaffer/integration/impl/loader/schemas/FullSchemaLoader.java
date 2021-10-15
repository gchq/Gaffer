/*
 * Copyright 2018-2020 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.types.FreqMap;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link SchemaLoader} implementation to load fully featured {@link uk.gov.gchq.gaffer.store.schema.Schema}s
 * for testing purposes.
 */
public class FullSchemaLoader implements SchemaLoader {

    @Override
    public Map<EdgeId, Edge> createEdges() {
        final Map<EdgeId, Edge> edges = new HashMap<>();
        for (int i = 0; i <= 10; i++) {
            for (int j = 0; j < VERTEX_PREFIXES.length; j++) {
                final FreqMap freqMap = new FreqMap();
                freqMap.upsert("key");
                final Edge edge = new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(VERTEX_PREFIXES[0] + i)
                        .dest(VERTEX_PREFIXES[j] + i)
                        .directed(false)
                        .property(TestPropertyNames.COUNT, 1L)
                        .property(TestPropertyNames.PROP_1, 1)
                        .property(TestPropertyNames.PROP_2, 1L)
                        .property(TestPropertyNames.PROP_3, "1")
                        .property(TestPropertyNames.PROP_4, freqMap)
                        .property(TestPropertyNames.PROP_5, "property")
                        .property(TestPropertyNames.DATE, Date.from(Instant.ofEpochMilli(1)))
                        .property(TestPropertyNames.TIMESTAMP, 1L)
                        .property(TestPropertyNames.VISIBILITY, "public")
                        .build();
                addToMap(edge, edges);

                final FreqMap freqMap2 = new FreqMap();
                freqMap2.upsert("key");
                final Edge edgeDir = new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(VERTEX_PREFIXES[0] + i)
                        .dest(VERTEX_PREFIXES[j] + i)
                        .directed(true)
                        .property(TestPropertyNames.COUNT, 1L)
                        .property(TestPropertyNames.PROP_1, 1)
                        .property(TestPropertyNames.PROP_2, 1L)
                        .property(TestPropertyNames.PROP_3, "1")
                        .property(TestPropertyNames.PROP_4, freqMap2)
                        .property(TestPropertyNames.PROP_5, "property")
                        .property(TestPropertyNames.DATE, Date.from(Instant.ofEpochMilli(1)))
                        .property(TestPropertyNames.TIMESTAMP, 1L)
                        .property(TestPropertyNames.VISIBILITY, "private")
                        .build();
                addToMap(edgeDir, edges);
            }

            final FreqMap freqMap3 = new FreqMap();
            freqMap3.upsert("key");
            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(SOURCE + i)
                    .dest(DEST + i)
                    .directed(false)
                    .property(TestPropertyNames.COUNT, 1L)
                    .property(TestPropertyNames.PROP_1, 1)
                    .property(TestPropertyNames.PROP_2, 1L)
                    .property(TestPropertyNames.PROP_3, "1")
                    .property(TestPropertyNames.PROP_4, freqMap3)
                    .property(TestPropertyNames.PROP_5, "property")
                    .property(TestPropertyNames.DATE, Date.from(Instant.ofEpochMilli(1)))
                    .property(TestPropertyNames.TIMESTAMP, 1L)
                    .property(TestPropertyNames.VISIBILITY, "public")
                    .build();
            addToMap(edge, edges);

            final FreqMap freqMap4 = new FreqMap();
            freqMap4.upsert("key");
            final Edge edgeDir = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(SOURCE_DIR + i)
                    .dest(DEST_DIR + i)
                    .directed(true)
                    .property(TestPropertyNames.COUNT, 1L)
                    .property(TestPropertyNames.PROP_1, 1)
                    .property(TestPropertyNames.PROP_2, 1L)
                    .property(TestPropertyNames.PROP_3, "1")
                    .property(TestPropertyNames.PROP_4, freqMap4)
                    .property(TestPropertyNames.PROP_5, "property")
                    .property(TestPropertyNames.DATE, Date.from(Instant.ofEpochMilli(1)))
                    .property(TestPropertyNames.TIMESTAMP, 1L)
                    .property(TestPropertyNames.VISIBILITY, "private")
                    .build();
            addToMap(edgeDir, edges);
        }

        return edges;
    }

    @Override
    public Map<EntityId, Entity> createEntities() {
        final Map<EntityId, Entity> entities = new HashMap<>();
        for (int i = 0; i <= 10; i++) {
            for (int j = 0; j < VERTEX_PREFIXES.length; j++) {
                final FreqMap freqMap = new FreqMap();
                freqMap.upsert("key");

                final Entity entity = new Entity(TestGroups.ENTITY, VERTEX_PREFIXES[j] + i);
                entity.putProperty(TestPropertyNames.COUNT, 1L);
                entity.putProperty(TestPropertyNames.PROP_1, 1);
                entity.putProperty(TestPropertyNames.PROP_2, 1L);
                entity.putProperty(TestPropertyNames.PROP_3, "1");
                entity.putProperty(TestPropertyNames.PROP_4, freqMap);
                entity.putProperty(TestPropertyNames.PROP_5, "property");
                entity.putProperty(TestPropertyNames.DATE, Date.from(Instant.ofEpochMilli(1)));
                entity.putProperty(TestPropertyNames.TIMESTAMP, 1L);
                entity.putProperty(TestPropertyNames.VISIBILITY, "public");
                addToMap(entity, entities);
            }

            final FreqMap freqMap2 = new FreqMap();
            freqMap2.upsert("key");
            final Entity secondEntity = new Entity(TestGroups.ENTITY, SOURCE + i);
            secondEntity.putProperty(TestPropertyNames.COUNT, 1L);
            secondEntity.putProperty(TestPropertyNames.PROP_1, 1);
            secondEntity.putProperty(TestPropertyNames.PROP_2, 1L);
            secondEntity.putProperty(TestPropertyNames.PROP_3, "1");
            secondEntity.putProperty(TestPropertyNames.PROP_4, freqMap2);
            secondEntity.putProperty(TestPropertyNames.PROP_5, "property");
            secondEntity.putProperty(TestPropertyNames.DATE, Date.from(Instant.ofEpochMilli(1)));
            secondEntity.putProperty(TestPropertyNames.TIMESTAMP, 1L);
            secondEntity.putProperty(TestPropertyNames.VISIBILITY, "public");
            addToMap(secondEntity, entities);

            final FreqMap freqMap3 = new FreqMap();
            freqMap3.upsert("key");
            final Entity thirdEntity = new Entity(TestGroups.ENTITY, DEST + i);
            thirdEntity.putProperty(TestPropertyNames.COUNT, 1L);
            thirdEntity.putProperty(TestPropertyNames.PROP_1, 1);
            thirdEntity.putProperty(TestPropertyNames.PROP_2, 1L);
            thirdEntity.putProperty(TestPropertyNames.PROP_3, "1");
            thirdEntity.putProperty(TestPropertyNames.PROP_4, freqMap3);
            thirdEntity.putProperty(TestPropertyNames.PROP_5, "property");
            thirdEntity.putProperty(TestPropertyNames.DATE, Date.from(Instant.ofEpochMilli(1)));
            thirdEntity.putProperty(TestPropertyNames.TIMESTAMP, 1L);
            thirdEntity.putProperty(TestPropertyNames.VISIBILITY, "private");
            addToMap(thirdEntity, entities);

            final FreqMap freqMap4 = new FreqMap();
            freqMap4.upsert("key");
            final Entity fourthEntity = new Entity(TestGroups.ENTITY, SOURCE_DIR + i);
            fourthEntity.putProperty(TestPropertyNames.COUNT, 1L);
            fourthEntity.putProperty(TestPropertyNames.PROP_1, 1);
            fourthEntity.putProperty(TestPropertyNames.PROP_2, 1L);
            fourthEntity.putProperty(TestPropertyNames.PROP_3, "1");
            fourthEntity.putProperty(TestPropertyNames.PROP_4, freqMap4);
            fourthEntity.putProperty(TestPropertyNames.PROP_5, "property");
            fourthEntity.putProperty(TestPropertyNames.DATE, Date.from(Instant.ofEpochMilli(1)));
            fourthEntity.putProperty(TestPropertyNames.TIMESTAMP, 1L);
            fourthEntity.putProperty(TestPropertyNames.VISIBILITY, "public");
            addToMap(fourthEntity, entities);

            final FreqMap freqMap5 = new FreqMap();
            freqMap5.upsert("key");
            final Entity fifthEntity = new Entity(TestGroups.ENTITY, DEST_DIR + i);
            fifthEntity.putProperty(TestPropertyNames.COUNT, 1L);
            fifthEntity.putProperty(TestPropertyNames.PROP_1, 1);
            fifthEntity.putProperty(TestPropertyNames.PROP_2, 1L);
            fifthEntity.putProperty(TestPropertyNames.PROP_3, "1");
            fifthEntity.putProperty(TestPropertyNames.PROP_4, freqMap5);
            fifthEntity.putProperty(TestPropertyNames.PROP_5, "property");
            fifthEntity.putProperty(TestPropertyNames.DATE, Date.from(Instant.ofEpochMilli(1)));
            fifthEntity.putProperty(TestPropertyNames.TIMESTAMP, 1L);
            fifthEntity.putProperty(TestPropertyNames.VISIBILITY, "private");
            addToMap(fifthEntity, entities);
        }

        return entities;
    }
}
