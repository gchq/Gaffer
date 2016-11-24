package uk.gov.gchq.gaffer.accumulostore.utils;

import com.google.common.collect.Sets;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import java.util.Collections;
import java.util.Set;

public class AccumuloTestData {

    public static final long TIMESTAMP = System.currentTimeMillis();
    public static final String TEST_OPTION_PROPERTY_KEY = "testOption";

    public static EntitySeed NOT_PRESENT_ENTITY_SEED = new EntitySeed("notpresent");
    public static Set<EntitySeed> NOT_PRESENT_ENTITY_SEED_SET = Collections.singleton(NOT_PRESENT_ENTITY_SEED);

    public static EntitySeed SEED_A = new EntitySeed("A");
    public static EntitySeed SEED_A0 = new EntitySeed("A0");
    public static EntitySeed SEED_A1 = new EntitySeed("A1");
    public static EntitySeed SEED_A2 = new EntitySeed("A2");
    public static EntitySeed SEED_A23 = new EntitySeed("A23");

    public static Set<EntitySeed> SEED_A_SET = Collections.singleton(SEED_A);
    public static Set<EntitySeed> SEED_A0_SET = Collections.singleton(SEED_A0);
    public static Set<EntitySeed> SEED_A1_SET = Collections.singleton(SEED_A1);
    public static Set<EntitySeed> SEED_A2_SET = Collections.singleton(SEED_A2);
    public static Set<EntitySeed> SEED_A23_SET = Collections.singleton(SEED_A23);
    public static Set<EntitySeed> SEED_A0_A23_SET = Sets.newHashSet(SEED_A0, SEED_A23);


    public static EntitySeed SEED_B = new EntitySeed("B");
    public static EntitySeed SEED_B1 = new EntitySeed("B1");
    public static EntitySeed SEED_B2 = new EntitySeed("B2");


    public static EntitySeed SEED_SOURCE_1 = new EntitySeed("source1");
    public static EntitySeed SEED_DESTINATION_1 = new EntitySeed("destination1");
    public static EntitySeed SEED_SOURCE_2 = new EntitySeed("source2");
    public static EntitySeed SEED_DESTINATION_2 = new EntitySeed("destination2");

    public static Set<EntitySeed> SEED_B_SET = Collections.singleton(SEED_B);
    public static Set<EntitySeed> SEED_B1_SET = Collections.singleton(SEED_B1);
    public static Set<EntitySeed> SEED_B2_SET = Collections.singleton(SEED_B2);

    public static Edge EDGE_A1_B1;
    public static Edge EDGE_B2_A2;

    public static Edge EDGE_A_B_1;
    public static Edge EDGE_A_B_2;

    public static Edge EDGE_A0_A23;

    public static Edge EDGE_C_D_UNDIRECTED;
    public static Edge EDGE_C_D_DIRECTED;

    public static Element A0_ENTITY;
    public static Element A1_ENTITY;
    public static Element A2_ENTITY;
    public static Element A23_ENTITY;

   static {

       EDGE_A1_B1 = new Edge(TestGroups.EDGE, "A1", "B1", true);
       EDGE_A1_B1.putProperty(AccumuloPropertyNames.COUNT, 1);

       EDGE_B2_A2 = new Edge(TestGroups.EDGE, "B2", "A2", true);

       EDGE_A0_A23 = new Edge(TestGroups.EDGE, "A0", "A23", true);
       EDGE_A0_A23.putProperty(AccumuloPropertyNames.COUNT, 23);
       EDGE_A0_A23.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);

       // Create directed edge A -> B and undirected edge A - B
       EDGE_A_B_1 = new Edge(TestGroups.EDGE, "A", "B", true);
       EDGE_A_B_2 = new Edge(TestGroups.EDGE, "A", "B", false);

       EDGE_A_B_1.putProperty(AccumuloPropertyNames.COUNT, 1);
       EDGE_A_B_2.putProperty(AccumuloPropertyNames.COUNT, 2);

       EDGE_C_D_UNDIRECTED = new Edge(TestGroups.EDGE);
       EDGE_C_D_UNDIRECTED.setSource("C");
       EDGE_C_D_UNDIRECTED.setDestination("D");
       EDGE_C_D_UNDIRECTED.setDirected(false);
       EDGE_C_D_UNDIRECTED.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
       EDGE_C_D_UNDIRECTED.putProperty(AccumuloPropertyNames.COUNT, 1);

       EDGE_C_D_DIRECTED = new Edge(TestGroups.EDGE);
       EDGE_C_D_DIRECTED.setSource("C");
       EDGE_C_D_DIRECTED.setDestination("D");
       EDGE_C_D_DIRECTED.setDirected(true);
       EDGE_C_D_DIRECTED.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 2);
       EDGE_C_D_DIRECTED.putProperty(AccumuloPropertyNames.COUNT, 1);

       A0_ENTITY = new Entity(TestGroups.ENTITY, "A0");
       A0_ENTITY.putProperty(AccumuloPropertyNames.COUNT, 10000);

       A1_ENTITY = new Entity(TestGroups.ENTITY, "A1");
       A1_ENTITY.putProperty(AccumuloPropertyNames.COUNT, 1);

       A2_ENTITY = new Entity(TestGroups.ENTITY, "A2");
       A2_ENTITY.putProperty(AccumuloPropertyNames.COUNT, 2);

       A23_ENTITY = new Entity(TestGroups.ENTITY, "A23");
       A23_ENTITY.putProperty(AccumuloPropertyNames.COUNT, 23);
    }

}
