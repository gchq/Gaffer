package uk.gov.gchq.gaffer.integration.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.function.simple.filter.AgeOff;
import uk.gov.gchq.gaffer.function.simple.filter.IsLessThan;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetEntitiesBySeed;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
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
                .type(TestTypes.TIMESTAMP, new TypeDefinition.Builder()
                        .validator(new ElementFilter.Builder()
                                .execute(new AgeOff(AGE_OFF_TIME))
                                .build())
                        .build())
                .type(TestTypes.PROP_INTEGER, new TypeDefinition.Builder()
                        .validator(new ElementFilter.Builder()
                                .execute(new IsLessThan(10))
                                .build())
                        .build())
                .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                        .property(TestPropertyNames.INT, TestTypes.PROP_INTEGER)
                        .build())
                .buildModule());

        return schema;
    }

    @Test
    @TraitRequirement(StoreTrait.TRANSFORMATION)
    public void shouldAgeOfDataBasedOnTimestampAndAgeOfFunctionInSchema() throws OperationException, InterruptedException {
        // Given
        final User user = new User();
        final long now = System.currentTimeMillis();
        final Entity entity = new Entity(TestGroups.ENTITY_2, VERTEX);
        entity.putProperty(TestPropertyNames.TIMESTAMP, now);
        entity.putProperty(TestPropertyNames.INT, 5);

        graph.execute(new AddElements.Builder()
                .elements(Collections.<Element>singleton(entity))
                .build(), user);

        // When 1 - before age off
        final CloseableIterable<Entity> results1 = graph.execute(new GetEntitiesBySeed.Builder()
                .addSeed(new EntitySeed(VERTEX))
                .build(), user);

        // Then 1
        final List<Entity> results1List = Lists.newArrayList(results1);
        assertEquals(1, results1List.size());
        assertEquals(VERTEX, results1List.get(0).getVertex());


        // Wait until after the age off time
        while (System.currentTimeMillis() - now < AGE_OFF_TIME) {
            Thread.sleep(1000L);
        }

        // When 2 - after age off
        final CloseableIterable<Entity> results2 = graph.execute(new GetEntitiesBySeed.Builder()
                .addSeed(new EntitySeed(VERTEX))
                .build(), user);

        // Then 2
        final List<Entity> results2List = Lists.newArrayList(results2);
        assertTrue(results2List.isEmpty());
    }

    @Test
    @TraitRequirement(StoreTrait.TRANSFORMATION)
    public void shouldRemoveInvalidElements() throws OperationException, InterruptedException {
        // Given
        final User user = new User();
        final Entity entity = new Entity(TestGroups.ENTITY_2, VERTEX);
        entity.putProperty(TestPropertyNames.INT, 100);

        // add elements but skip the validation
        graph.execute(new AddElements.Builder()
                .elements(Collections.<Element>singleton(entity))
                .validate(false)
                .build(), user);

        // When
        final CloseableIterable<Entity> results1 = graph.execute(new GetEntitiesBySeed.Builder()
                .addSeed(new EntitySeed(VERTEX))
                .build(), user);

        // Then
        final List<Entity> results1List = Lists.newArrayList(results1);
        assertTrue(results1List.isEmpty());
    }
}
