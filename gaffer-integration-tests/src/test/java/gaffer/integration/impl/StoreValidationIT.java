package gaffer.integration.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.element.function.ElementFilter;
import gaffer.function.simple.aggregate.Max;
import gaffer.function.simple.filter.AgeOff;
import gaffer.function.simple.filter.IsLessThan;
import gaffer.integration.AbstractStoreIT;
import gaffer.integration.TraitRequirement;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetEntitiesBySeed;
import gaffer.store.StoreTrait;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaEntityDefinition;
import gaffer.store.schema.TypeDefinition;
import org.junit.Test;
import java.util.Collections;
import java.util.List;

public class StoreValidationIT extends AbstractStoreIT {
    private static final String VERTEX = "vertex";
    private static final long AGE_OFF_TIME = 4L * 1000; // 4 seconds;

    @Override
    protected Schema createSchema() {
        final Schema schema = super.createSchema();
        schema.merge(new Schema.Builder()
                .type("timestamp", new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .validator(new ElementFilter.Builder()
                                .execute(new AgeOff(AGE_OFF_TIME))
                                .build())
                        .aggregateFunction(new Max())
                        .build())
                .type("prop.integer", new TypeDefinition.Builder()
                        .validator(new ElementFilter.Builder()
                                .execute(new IsLessThan(10))
                                .build())
                        .build())
                .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.TIMESTAMP, "timestamp")
                        .property(TestPropertyNames.INT, "prop.integer")
                        .build())
                .buildModule());

        return schema;
    }

    @Test
    @TraitRequirement(StoreTrait.TRANSFORMATION)
    public void shouldAgeOfDataBasedOnTimestampAndAgeOfFunctionInSchema() throws OperationException, InterruptedException {
        // Given
        final long now = System.currentTimeMillis();
        final Entity entity = new Entity(TestGroups.ENTITY_2, VERTEX);
        entity.putProperty(TestPropertyNames.TIMESTAMP, now);
        entity.putProperty(TestPropertyNames.INT, 5);

        graph.execute(new AddElements.Builder()
                .elements(Collections.<Element>singleton(entity))
                .build());

        // When 1 - before age off
        final Iterable<Entity> results1 = graph.execute(new GetEntitiesBySeed.Builder()
                .addSeed(new EntitySeed(VERTEX))
                .build());

        // Then 1
        final List<Entity> results1List = Lists.newArrayList(results1);
        assertEquals(1, results1List.size());
        assertEquals(VERTEX, results1List.get(0).getVertex());


        // Wait until after the age off time
        while (System.currentTimeMillis() - now < AGE_OFF_TIME) {
            Thread.sleep(1000L);
        }

        // When 2 - after age off
        final Iterable<Entity> results2 = graph.execute(new GetEntitiesBySeed.Builder()
                .addSeed(new EntitySeed(VERTEX))
                .build());

        // Then 2
        final List<Entity> results2List = Lists.newArrayList(results2);
        assertTrue(results2List.isEmpty());
    }

    @Test
    @TraitRequirement(StoreTrait.TRANSFORMATION)
    public void shouldRemoveInvalidElements() throws OperationException, InterruptedException {
        // Given
        final Entity entity = new Entity(TestGroups.ENTITY_2, VERTEX);
        entity.putProperty(TestPropertyNames.INT, 100);

        // add elements but skip the validation
        graph.execute(new AddElements.Builder()
                .elements(Collections.<Element>singleton(entity))
                .validate(false)
                .build());

        // When
        final Iterable<Entity> results1 = graph.execute(new GetEntitiesBySeed.Builder()
                .addSeed(new EntitySeed(VERTEX))
                .build());

        // Then
        final List<Entity> results1List = Lists.newArrayList(results1);
        assertTrue(results1List.isEmpty());
    }
}
