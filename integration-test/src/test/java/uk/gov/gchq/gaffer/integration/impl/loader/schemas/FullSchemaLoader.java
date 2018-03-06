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

package uk.gov.gchq.gaffer.integration.impl.loader.schemas;

import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.integration.impl.loader.schemas.SchemaLoader;
import uk.gov.gchq.gaffer.types.FreqMap;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class FullSchemaLoader implements SchemaLoader {

    @Override
    public Map<EdgeId, Edge> createEdges() {
        final Map<String, Long> map = new HashMap<>();
        map.put("key", 1L);

        final FreqMap freqMap = new FreqMap(map);

        final Map<EdgeId, Edge> edges = new HashMap<>();
        for (int i = 0; i <= 10; i++) {
            for (int j = 0; j < VERTEX_PREFIXES.length; j++) {
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
                        .property(TestPropertyNames.PROP_5, Sets.newHashSet("property"))
                        .property(TestPropertyNames.DATE, Date.from(Instant.ofEpochMilli(1)))
                        .property(TestPropertyNames.TIMESTAMP, 1L)
                        .property(TestPropertyNames.VISIBILITY, "public")
                        .build();
                addToMap(edge, edges);

                final Edge edgeDir = new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(VERTEX_PREFIXES[0] + i)
                        .dest(VERTEX_PREFIXES[j] + i)
                        .directed(true)
                        .property(TestPropertyNames.COUNT, 1L)
                        .property(TestPropertyNames.PROP_1, 1)
                        .property(TestPropertyNames.PROP_2, 1L)
                        .property(TestPropertyNames.PROP_3, "1")
                        .property(TestPropertyNames.PROP_4, freqMap)
                        .property(TestPropertyNames.PROP_5, Sets.newHashSet("property"))
                        .property(TestPropertyNames.DATE, Date.from(Instant.ofEpochMilli(1)))
                        .property(TestPropertyNames.TIMESTAMP, 1L)
                        .property(TestPropertyNames.VISIBILITY, "public")
                        .build();
                addToMap(edgeDir, edges);
            }

            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(SOURCE + i)
                    .dest(DEST + i)
                    .directed(false)
                    .property(TestPropertyNames.COUNT, 1L)
                    .property(TestPropertyNames.PROP_1, 1)
                    .property(TestPropertyNames.PROP_2, 1L)
                    .property(TestPropertyNames.PROP_3, "1")
                    .property(TestPropertyNames.PROP_4, freqMap)
                    .property(TestPropertyNames.PROP_5, Sets.newHashSet("property"))
                    .property(TestPropertyNames.DATE, Date.from(Instant.ofEpochMilli(1)))
                    .property(TestPropertyNames.TIMESTAMP, 1L)
                    .property(TestPropertyNames.VISIBILITY, "public")
                    .build();
            addToMap(edge, edges);

            final Edge edgeDir = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(SOURCE_DIR + i)
                    .dest(DEST_DIR + i)
                    .directed(true)
                    .property(TestPropertyNames.COUNT, 1L)
                    .property(TestPropertyNames.PROP_1, 1)
                    .property(TestPropertyNames.PROP_2, 1L)
                    .property(TestPropertyNames.PROP_3, "1")
                    .property(TestPropertyNames.PROP_4, freqMap)
                    .property(TestPropertyNames.PROP_5, Sets.newHashSet("property"))
                    .property(TestPropertyNames.DATE, Date.from(Instant.ofEpochMilli(1)))
                    .property(TestPropertyNames.TIMESTAMP, 1L)
                    .property(TestPropertyNames.VISIBILITY, "public")
                    .build();
            addToMap(edgeDir, edges);
        }

        return edges;
    }

    @Override
    public Map<EntityId, Entity> createEntities() {
        final FreqMap freqMap = new FreqMap();
        freqMap.upsert("key");

        final Map<EntityId, Entity> entities = new HashMap<>();
        for (int i = 0; i <= 10; i++) {
            for (int j = 0; j < VERTEX_PREFIXES.length; j++) {
                final Entity entity = new Entity(TestGroups.ENTITY, VERTEX_PREFIXES[j] + i);
                entity.putProperty(TestPropertyNames.COUNT, 1L);
                entity.putProperty(TestPropertyNames.PROP_1, 1);
                entity.putProperty(TestPropertyNames.PROP_2, 1L);
                entity.putProperty(TestPropertyNames.PROP_3, "1");
                entity.putProperty(TestPropertyNames.PROP_4, freqMap);
                entity.putProperty(TestPropertyNames.PROP_5, Sets.newHashSet("property"));
                entity.putProperty(TestPropertyNames.DATE, Date.from(Instant.ofEpochMilli(1)));
                entity.putProperty(TestPropertyNames.TIMESTAMP, 1L);
                entity.putProperty(TestPropertyNames.VISIBILITY, "public");
                addToMap(entity, entities);
            }

            final Entity secondEntity = new Entity(TestGroups.ENTITY, SOURCE + i);
            secondEntity.putProperty(TestPropertyNames.COUNT, 1L);
            secondEntity.putProperty(TestPropertyNames.PROP_1, 1);
            secondEntity.putProperty(TestPropertyNames.PROP_2, 1L);
            secondEntity.putProperty(TestPropertyNames.PROP_3, "1");
            secondEntity.putProperty(TestPropertyNames.PROP_4, freqMap);
            secondEntity.putProperty(TestPropertyNames.PROP_5, Sets.newHashSet("property"));
            secondEntity.putProperty(TestPropertyNames.DATE, Date.from(Instant.ofEpochMilli(1)));
            secondEntity.putProperty(TestPropertyNames.TIMESTAMP, 1L);
            secondEntity.putProperty(TestPropertyNames.VISIBILITY, "public");
            addToMap(secondEntity, entities);

            final Entity thirdEntity = new Entity(TestGroups.ENTITY, DEST + i);
            thirdEntity.putProperty(TestPropertyNames.COUNT, 1L);
            thirdEntity.putProperty(TestPropertyNames.PROP_1, 1);
            thirdEntity.putProperty(TestPropertyNames.PROP_2, 1L);
            thirdEntity.putProperty(TestPropertyNames.PROP_3, "1");
            thirdEntity.putProperty(TestPropertyNames.PROP_4, freqMap);
            thirdEntity.putProperty(TestPropertyNames.PROP_5, Sets.newHashSet("property"));
            thirdEntity.putProperty(TestPropertyNames.DATE, Date.from(Instant.ofEpochMilli(1)));
            thirdEntity.putProperty(TestPropertyNames.TIMESTAMP, 1L);
            thirdEntity.putProperty(TestPropertyNames.VISIBILITY, "public");
            addToMap(thirdEntity, entities);

            final Entity fourthEntity = new Entity(TestGroups.ENTITY, SOURCE_DIR + i);
            fourthEntity.putProperty(TestPropertyNames.COUNT, 1L);
            fourthEntity.putProperty(TestPropertyNames.PROP_1, 1);
            fourthEntity.putProperty(TestPropertyNames.PROP_2, 1L);
            fourthEntity.putProperty(TestPropertyNames.PROP_3, "1");
            fourthEntity.putProperty(TestPropertyNames.PROP_4, freqMap);
            fourthEntity.putProperty(TestPropertyNames.PROP_5, Sets.newHashSet("property"));
            fourthEntity.putProperty(TestPropertyNames.DATE, Date.from(Instant.ofEpochMilli(1)));
            fourthEntity.putProperty(TestPropertyNames.TIMESTAMP, 1L);
            fourthEntity.putProperty(TestPropertyNames.VISIBILITY, "public");
            addToMap(fourthEntity, entities);

            final Entity fifthEntity = new Entity(TestGroups.ENTITY, DEST_DIR + i);
            fifthEntity.putProperty(TestPropertyNames.COUNT, 1L);
            fifthEntity.putProperty(TestPropertyNames.PROP_1, 1);
            fifthEntity.putProperty(TestPropertyNames.PROP_2, 1L);
            fifthEntity.putProperty(TestPropertyNames.PROP_3, "1");
            fifthEntity.putProperty(TestPropertyNames.PROP_4, freqMap);
            fifthEntity.putProperty(TestPropertyNames.PROP_5, Sets.newHashSet("property"));
            fifthEntity.putProperty(TestPropertyNames.DATE, Date.from(Instant.ofEpochMilli(1)));
            fifthEntity.putProperty(TestPropertyNames.TIMESTAMP, 1L);
            fifthEntity.putProperty(TestPropertyNames.VISIBILITY, "public");
            addToMap(fifthEntity, entities);
        }

        return entities;
    }
}
