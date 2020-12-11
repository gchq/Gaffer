package uk.gov.gchq.gaffer.integration.provider;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.user.User;

import java.util.HashMap;
import java.util.Map;

import static uk.gov.gchq.gaffer.integration.util.TestUtil.DEST;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.DEST_DIR;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.SOURCE;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.SOURCE_DIR;

public class PopulatedGraphProvider extends EmptyGraphProvider {


    // Identifier prefixes

    private static final String A = "A";
    private static final String B = "B";
    private static final String C = "C";
    private static final String D = "D";
    private static final String[] VERTEX_PREFIXES = new String[]{A, B, C, D};

    private static final Map<EdgeId, Edge> EDGES = createDefaultEdges();
    private static final Map<EntityId, Entity> ENTITIES = createDefaultEntities();

    public PopulatedGraphProvider() {
        super();
        getGraphs()
                .forEach(graph -> {
                    try {
                        graph.execute(new AddElements.Builder()
                                .input(ENTITIES.values())
                                .build(), new User());

                        graph.execute(new AddElements.Builder()
                                .input(EDGES.values())
                                .build(), new User());
                    } catch (OperationException e) {
                        throw new RuntimeException("Failed to populate graph", e);
                    }
                });
    }

    private static void addToMap(final Edge element, final Map<EdgeId, Edge> edges) {
        edges.put(ElementSeed.createSeed(element), element);
    }

    private static void addToMap(final Entity element, final Map<EntityId, Entity> entities) {
        entities.put(ElementSeed.createSeed(element), element);
    }

    private static Map<EdgeId, Edge> createDefaultEdges() {
        final Map<EdgeId, Edge> edges = new HashMap<>();
        for (int i = 0; i <= 10; i++) {
            assert VERTEX_PREFIXES != null;
            for (int j = 0; j < VERTEX_PREFIXES.length; j++) {
                final Edge edge = new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(VERTEX_PREFIXES[0] + i)
                        .dest(VERTEX_PREFIXES[j] + i)
                        .directed(false)
                        .property(TestPropertyNames.INT, 1)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build();
                addToMap(edge, edges);

                final Edge edgeDir = new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(VERTEX_PREFIXES[0] + i)
                        .dest(VERTEX_PREFIXES[j] + i)
                        .directed(true)
                        .property(TestPropertyNames.INT, 1)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build();
                addToMap(edgeDir, edges);
            }

            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(SOURCE + i)
                    .dest(DEST + i)
                    .directed(false)
                    .property(TestPropertyNames.INT, 1)
                    .property(TestPropertyNames.COUNT, 1L)
                    .build();
            addToMap(edge, edges);

            final Edge edgeDir = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(SOURCE_DIR + i)
                    .dest(DEST_DIR + i)
                    .directed(true)
                    .build();
            edgeDir.putProperty(TestPropertyNames.INT, 1);
            edgeDir.putProperty(TestPropertyNames.COUNT, 1L);
            addToMap(edgeDir, edges);
        }

        return edges;
    }

    private static Map<EntityId, Entity> createDefaultEntities() {
        final Map<EntityId, Entity> entities = new HashMap<>();
        for (int i = 0; i <= 10; i++) {
            for (int j = 0; j < VERTEX_PREFIXES.length; j++) {
                final Entity entity = new Entity(TestGroups.ENTITY, VERTEX_PREFIXES[j] + i);
                entity.putProperty(TestPropertyNames.COUNT, 1L);
                entity.putProperty(TestPropertyNames.SET, CollectionUtil.treeSet("3"));
                addToMap(entity, entities);
            }

            final Entity secondEntity = new Entity(TestGroups.ENTITY, SOURCE + i);
            secondEntity.putProperty(TestPropertyNames.COUNT, 1L);
            secondEntity.putProperty(TestPropertyNames.SET, CollectionUtil.treeSet("3"));
            addToMap(secondEntity, entities);

            final Entity thirdEntity = new Entity(TestGroups.ENTITY, DEST + i);
            thirdEntity.putProperty(TestPropertyNames.COUNT, 1L);
            thirdEntity.putProperty(TestPropertyNames.SET, CollectionUtil.treeSet("3"));
            addToMap(thirdEntity, entities);

            final Entity fourthEntity = new Entity(TestGroups.ENTITY, SOURCE_DIR + i);
            fourthEntity.putProperty(TestPropertyNames.COUNT, 1L);
            fourthEntity.putProperty(TestPropertyNames.SET, CollectionUtil.treeSet("3"));
            addToMap(fourthEntity, entities);

            final Entity fifthEntity = new Entity(TestGroups.ENTITY, DEST_DIR + i);
            fifthEntity.putProperty(TestPropertyNames.COUNT, 1L);
            fifthEntity.putProperty(TestPropertyNames.SET, CollectionUtil.treeSet("3"));
            addToMap(fifthEntity, entities);
        }

        return entities;
    }
}
