package gaffer.accumulostore.integration;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import gaffer.accumulostore.AccumuloProperties;
import gaffer.accumulostore.utils.StorePositions;
import gaffer.commonutil.StreamUtil;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.schema.DataEntityDefinition;
import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.function.simple.filter.AgeOff;
import gaffer.graph.Graph;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetEntitiesBySeed;
import gaffer.store.schema.StoreElementDefinition;
import gaffer.store.schema.StorePropertyDefinition;
import gaffer.store.schema.StoreSchema;
import org.junit.Test;
import java.util.Collections;
import java.util.List;

public class AgeOffIT {
    @Test
    public void shouldAgeOffDataBasedOnSchema() throws OperationException, InterruptedException {
        // Given
        final long now = System.currentTimeMillis();
        final String vertex = "entity1";
        final long ageOffTime = 4L * 1000; // 4 seconds;
        final DataSchema dataSchema = new DataSchema.Builder()
                .entity(TestGroups.ENTITY, new DataEntityDefinition.Builder()
                        .property(TestPropertyNames.TIMESTAMP, Long.class)
                        .validator(new ElementFilter.Builder()
                                .execute(new AgeOff(ageOffTime))
                                .select(TestPropertyNames.TIMESTAMP)
                                .build())
                        .build())
                .build();

        final StoreSchema storeSchema = new StoreSchema.Builder()
                .entity(TestGroups.ENTITY, new StoreElementDefinition.Builder()
                        .property(TestPropertyNames.TIMESTAMP, new StorePropertyDefinition.Builder()
                                .position(StorePositions.VALUE.name())
                                .build())
                        .build())
                .build();

        final Graph graph = new Graph(dataSchema, storeSchema,
                AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(getClass())));

        final Entity entity = new Entity(TestGroups.ENTITY, vertex);
        entity.putProperty(TestPropertyNames.TIMESTAMP, now);

        graph.execute(new AddElements.Builder()
                .elements(Collections.<Element>singleton(entity))
                .build());

        // When 1 - before age off
        final Iterable<Entity> results1 = graph.execute(new GetEntitiesBySeed.Builder()
                .addSeed(new EntitySeed(vertex))
                .build());

        // Then 1
        final List<Entity> results1List = Lists.newArrayList(results1);
        assertEquals(1, results1List.size());
        assertEquals(vertex, results1List.get(0).getVertex());


        // Wait until after the age off time
        while (System.currentTimeMillis() - now < ageOffTime) {
            Thread.sleep(1000L);
        }

        // When 2 - after age off
        final Iterable<Entity> results2 = graph.execute(new GetEntitiesBySeed.Builder()
                .addSeed(new EntitySeed(vertex))
                .build());

        // Then 2
        final List<Entity> results2List = Lists.newArrayList(results2);
        assertEquals(0, results2List.size());
    }
}
