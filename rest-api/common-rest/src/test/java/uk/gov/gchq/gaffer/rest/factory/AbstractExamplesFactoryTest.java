package uk.gov.gchq.gaffer.rest.factory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.compare.Max;
import uk.gov.gchq.gaffer.operation.impl.compare.Min;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AbstractExamplesFactoryTest {

    private static final Schema SCHEMA  = new Schema.Builder()
            .json(StreamUtil.schema(TestExamplesFactory.class))
            .build();

    @Test
    public void shouldUseSchemaToCreateGetElementsInput() throws InstantiationException, IllegalAccessException {
        // Given
        TestExamplesFactory examplesFactory = new TestExamplesFactory(SCHEMA);

        // When
        GetElements operation = (GetElements) examplesFactory.generateExample(GetElements.class);

        // Then
        int size = 0;
        for (ElementId e : operation.getInput()) {
            size++;
            if (e instanceof EntityId) {
                assertEquals(String.class, ((EntityId) e).getVertex().getClass());
            } else {
                assertEquals(String.class, ((EdgeId) e).getDestination().getClass());
                assertEquals(String.class, ((EdgeId) e).getSource().getClass());
            }
        }
        assertEquals(2, size);
    }

    @Test
    public void shouldUseSchemaToCreateGetAdjacentIdsInput() throws InstantiationException, IllegalAccessException {
        // Given
        TestExamplesFactory examplesFactory = new TestExamplesFactory(SCHEMA);

        // When
        GetAdjacentIds operation = (GetAdjacentIds) examplesFactory.generateExample(GetAdjacentIds.class);

        // Then
        int size = 0;
        for (ElementId e : operation.getInput()) {
            size++;
            if (e instanceof EntityId) {
                assertEquals(String.class, ((EntityId) e).getVertex().getClass());
            } else {
                throw new RuntimeException("Expected operation only to contain entity ids");
            }
        }
        assertEquals(1, size);
    }

    @Test
    public void shouldPopulateAddElementsAccordingToSchema() throws InstantiationException, IllegalAccessException {
        // Given
        TestExamplesFactory examplesFactory = new TestExamplesFactory(SCHEMA);

        // When
        AddElements operation = (AddElements) examplesFactory.generateExample(AddElements.class);

        // Then
        ArrayList<Element> expectedInput = Lists.newArrayList(
                new Entity.Builder()
                        .group("BasicEntity")
                        .vertex("vertex1")
                        .property("count", 1)
                        .build(),
                new Entity.Builder()
                        .group("BasicEntity")
                        .vertex("vertex2")
                        .property("count", 2)
                        .build(),
                new Edge.Builder()
                        .group("BasicEdge")
                        .source("vertex1")
                        .dest("vertex2")
                        .directed(true)
                        .property("count", 1)
                        .build()
        );

        assertEquals(expectedInput, Lists.newArrayList(operation.getInput()));
    }

    @Test
    public void shouldUseSchemaForGroupsInSortOperation() throws InstantiationException, IllegalAccessException {
        // Given
        TestExamplesFactory examplesFactory = new TestExamplesFactory(SCHEMA);

        // When
        Sort operation = (Sort) examplesFactory.generateExample(Sort.class);

        // Then
        // Sort has no equals method
        assertEquals(1, operation.getComparators().size());
        assertEquals(Sets.newHashSet("BasicEdge"), ((ElementPropertyComparator) operation.getComparators().get(0)).getGroups());
        assertEquals("count", ((ElementPropertyComparator) operation.getComparators().get(0)).getProperty());
    }

    @Test
    public void shouldUseSchemaForMaxOperation() throws InstantiationException, IllegalAccessException {
        // Given
        TestExamplesFactory examplesFactory = new TestExamplesFactory(SCHEMA);

        // When
        Max operation = (Max) examplesFactory.generateExample(Max.class);

        // Then
        // Max has no equals method
        assertEquals(1, operation.getComparators().size());
        assertEquals(Sets.newHashSet("BasicEdge"), ((ElementPropertyComparator) operation.getComparators().get(0)).getGroups());
        assertEquals("count", ((ElementPropertyComparator) operation.getComparators().get(0)).getProperty());
    }

    @Test
    public void shouldUseSchemaForMinOperation() throws InstantiationException, IllegalAccessException {
        // Given
        TestExamplesFactory examplesFactory = new TestExamplesFactory(SCHEMA);

        // When
        Min operation = (Min) examplesFactory.generateExample(Min.class);

        // Then
        // Min has no equals method
        assertEquals(1, operation.getComparators().size());
        assertEquals(Sets.newHashSet("BasicEdge"), ((ElementPropertyComparator) operation.getComparators().get(0)).getGroups());
        assertEquals("count", ((ElementPropertyComparator) operation.getComparators().get(0)).getProperty());
    }

    @Test
    public void shouldProvideEmptyGetWalksIfSchemaContainsNoEdges() throws InstantiationException, IllegalAccessException {
        // Given
        TestExamplesFactory examplesFactory = new TestExamplesFactory(new Schema());

        // When
        GetWalks operation = (GetWalks) examplesFactory.generateExample(GetWalks.class);

        // Then
        assertNull(operation.getInput());
        assertEquals(0, operation.getOperations().size());
    }

    @Test
    public void shouldProvideSchemaPopulatedGetWalksIfSchemaContainsEdges() throws InstantiationException, IllegalAccessException {
        // Given
        TestExamplesFactory examplesFactory = new TestExamplesFactory(SCHEMA);

        // When
        GetWalks operation = (GetWalks) examplesFactory.generateExample(GetWalks.class);

        // Then
        assertEquals(Lists.newArrayList(new EntitySeed("vertex1")), Lists.newArrayList(operation.getInput()));
        assertEquals(Lists.newArrayList(new OperationChain.Builder()
                .first(
                        new GetElements.Builder()
                                .view(new View.Builder()
                                        .edge("BasicEdge")
                                        .build())
                                .build())
                .build()), operation.getOperations());
    }

    private static class TestExamplesFactory extends AbstractExamplesFactory {

        private Schema schema;

        public TestExamplesFactory(final Schema schema) {
            this.schema = schema;
        }

        @Override
        protected Schema getSchema() {
            return this.schema;
        }
    }
}
