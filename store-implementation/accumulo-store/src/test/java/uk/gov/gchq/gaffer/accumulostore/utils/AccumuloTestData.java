/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.utils;

import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;

import java.util.Collections;
import java.util.Set;

public final class AccumuloTestData {
    public static final long TIMESTAMP = System.currentTimeMillis();
    public static final String TEST_OPTION_PROPERTY_KEY = "testOption";

    public static final EntityId NOT_PRESENT_ENTITY_SEED = new EntitySeed("notpresent");
    public static final Set<EntityId> NOT_PRESENT_ENTITY_SEED_SET = Collections.singleton(NOT_PRESENT_ENTITY_SEED);

    public static final EntityId SEED_A = new EntitySeed("A");
    public static final EntityId SEED_A0 = new EntitySeed("A0");
    public static final EntityId SEED_A1 = new EntitySeed("A1");
    public static final EntityId SEED_A2 = new EntitySeed("A2");
    public static final EntityId SEED_A23 = new EntitySeed("A23");

    public static final Set<EntityId> SEED_A_SET = Collections.singleton(SEED_A);
    public static final Set<EntityId> SEED_A0_SET = Collections.singleton(SEED_A0);
    public static final Set<EntityId> SEED_A1_SET = Collections.singleton(SEED_A1);
    public static final Set<EntityId> SEED_A2_SET = Collections.singleton(SEED_A2);
    public static final Set<EntityId> SEED_A23_SET = Collections.singleton(SEED_A23);
    public static final Set<EntityId> SEED_A0_A23_SET = Sets.newHashSet(SEED_A0, SEED_A23);

    public static final EntityId SEED_B = new EntitySeed("B");
    public static final EntityId SEED_B1 = new EntitySeed("B1");
    public static final EntityId SEED_B2 = new EntitySeed("B2");

    public static final EntityId SEED_SOURCE_1 = new EntitySeed("source1");
    public static final EntityId SEED_DESTINATION_1 = new EntitySeed("destination1");
    public static final EntityId SEED_SOURCE_2 = new EntitySeed("source2");
    public static final EntityId SEED_DESTINATION_2 = new EntitySeed("destination2");

    public static final Set<EntityId> SEED_B_SET = Collections.singleton(SEED_B);
    public static final Set<EntityId> SEED_B1_SET = Collections.singleton(SEED_B1);
    public static final Set<EntityId> SEED_B2_SET = Collections.singleton(SEED_B2);

    public static final Edge EDGE_A1_B1 =
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A1")
                    .dest("B1")
                    .directed(true)
                    .property(AccumuloPropertyNames.COUNT, 1)
                    .build();

    public static final Edge EDGE_B2_A2 =
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("B2")
                    .dest("A2")
                    .directed(true)
                    .build();

    public static final Edge EDGE_A_B_1 =
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A")
                    .dest("B")
                    .directed(true)
                    .property(AccumuloPropertyNames.COUNT, 1)
                    .build();

    public static final Edge EDGE_A_B_2 =
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A")
                    .dest("B")
                    .directed(false)
                    .property(AccumuloPropertyNames.COUNT, 2)
                    .build();

    public static final Edge EDGE_A0_A23 =
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A0")
                    .dest("A23")
                    .directed(true)
                    .property(AccumuloPropertyNames.COUNT, 23)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .build();

    public static final Edge EDGE_C_D_UNDIRECTED =
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("C")
                    .dest("D")
                    .directed(false)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.COUNT, 1)
                    .build();

    public static final Edge EDGE_C_D_DIRECTED =
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("C")
                    .dest("D")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 2)
                    .property(AccumuloPropertyNames.COUNT, 1).build();

    public static final Element A0_ENTITY =
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("A0")
                    .property(AccumuloPropertyNames.COUNT, 10000)
                    .build();

    public static final Element A1_ENTITY =
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("A1")
                    .property(AccumuloPropertyNames.COUNT, 1)
                    .build();

    public static final Element A2_ENTITY =
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("A2")
                    .property(AccumuloPropertyNames.COUNT, 2)
                    .build();

    public static final Element A23_ENTITY =
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("A23")
                    .property(AccumuloPropertyNames.COUNT, 23)
                    .build();

    private AccumuloTestData() {
        // private to prevent instantiation
    }
}
