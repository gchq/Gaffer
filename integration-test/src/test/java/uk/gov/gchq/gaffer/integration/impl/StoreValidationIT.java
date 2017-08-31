package uk.gov.gchq.gaffer.integration.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StoreValidationIT extends AbstractStoreIT {
    private static final String VERTEX = "vertex";

    @Test
    @TraitRequirement(StoreTrait.STORE_VALIDATION)
    public void shouldAgeOfDataBasedOnTimestampAndAgeOfFunctionInSchema() throws OperationException, InterruptedException {
        // Given
        final User user = new User();
        final long now = System.currentTimeMillis();
        final Entity entity = new Entity(TestGroups.ENTITY_2, VERTEX);
        entity.putProperty(TestPropertyNames.TIMESTAMP, now);
        entity.putProperty(TestPropertyNames.INT, 5);

        graph.execute(new AddElements.Builder()
                .input(entity)
                .build(), user);

        // When 1 - before age off
        final CloseableIterable<? extends Element> results1 = graph.execute(new GetElements.Builder()
                .input(new EntitySeed(VERTEX))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY_2)
                        .build())
                .build(), user);

        // Then 1
        final List<Element> results1List = Lists.newArrayList(results1);
        assertEquals(1, results1List.size());
        assertEquals(VERTEX, ((Entity) results1List.get(0)).getVertex());


        // Wait until after the age off time
        while (System.currentTimeMillis() - now < AGE_OFF_TIME) {
            Thread.sleep(1000L);
        }

        // When 2 - after age off
        final CloseableIterable<? extends Element> results2 = graph.execute(new GetElements.Builder()
                .input(new EntitySeed(VERTEX))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY_2)
                        .build())
                .build(), user);

        // Then 2
        final List<Element> results2List = Lists.newArrayList(results2);
        assertTrue(results2List.isEmpty());
    }

    @Test
    @TraitRequirement(StoreTrait.STORE_VALIDATION)
    public void shouldRemoveInvalidElements() throws OperationException, InterruptedException {
        // Given
        final User user = new User();
        final Entity entity = new Entity(TestGroups.ENTITY_2, VERTEX);
        entity.putProperty(TestPropertyNames.INT, 100);

        // add elements but skip the validation
        graph.execute(new AddElements.Builder()
                .input(Collections.<Element>singleton(entity))
                .validate(false)
                .build(), user);

        // When
        final CloseableIterable<? extends Element> results1 = graph.execute(new GetElements.Builder()
                .input(new EntitySeed(VERTEX))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY_2)
                        .build())
                .build(), user);

        // Then
        final List<Element> results1List = Lists.newArrayList(results1);
        assertTrue(results1List.isEmpty());
    }
}
