package uk.gov.gchq.gaffer.store.util;


import com.google.common.collect.Lists;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.function.ExampleFilterFunction;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaTest;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AggregatorUtilTest {

    @Test
    public void shouldUseVertexAsPartOfGroupBy() {
        // given
        final Schema schema = Schema.fromJson(StreamUtil.openStream(getClass(), "/schema/dataSchema.json"));

        // when
        final Function<Element, Element> fn = new AggregatorUtil.ToIngestElementKey(schema);

        final List<Element> input = Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex2")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.ENTITY)
                        .source("vertex2")
                        .dest("vertex1")
                        .build()
        );

        // then
        final Map<Element, List<Element>> results = input.stream().collect(Collectors.groupingBy(fn));
        final Map<Element, List<Element>> expected = new HashMap<>();
        expected.put(input.get(0), Lists.newArrayList(input.get(0)));
        expected.put(input.get(1), Lists.newArrayList(input.get(1)));
        expected.put(input.get(2), Lists.newArrayList(input.get(2)));
        assertEquals(expected, results);
    }

    @Test
    public void shouldUseGroupAsPartOfTheGroupByFunction() {
        // given
        final Schema schema = createSchema();

        // when
        final List<Element> input = Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY_2)
                        .vertex("vertex1")
                        .build()
        );

        final Map<Element, List<Element>> results = input.stream()
                .collect(Collectors.groupingBy(new AggregatorUtil.ToIngestElementKey(schema)));

        assertEquals(2, results.size());
        assertEquals(input.get(0), results.get(input.get(0)).get(0));
        assertEquals(input.get(1), results.get(input.get(1)).get(0));
    }

    @Test
    public void shouldUseGroupByPropertiesInFunction() {
        // given
        final Schema schema = createSchema();

        // when
        final Function<Element, Element> fn = new AggregatorUtil.ToIngestElementKey(schema);
        List<Element> input = Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.ENTITY_2)
                        .vertex("vertex1")
                        .property(TestPropertyNames.PROP_1, "test1")
                        .property(TestPropertyNames.PROP_2, 1)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY_2)
                        .vertex("vertex2")
                        .property(TestPropertyNames.PROP_1, "test1")
                        .property(TestPropertyNames.PROP_2, 2)
                        .build()
        );

        // then
        final Map<Element, List<Element>> results = input.stream().collect(Collectors.groupingBy(fn));
        final Map<Element, List<Element>> expected = new HashMap<>();
        expected.put(input.get(0), Lists.newArrayList(input.get(0)));
        expected.put(input.get(1), Lists.newArrayList(input.get(1)));
        assertEquals(expected, results);
    }

    @Test
    public void shouldThrowExceptionIfElementBelongsToGroupThatDoesntExistInSchema() {
        // given
        final Schema schema = createSchema();

        // when
        final List<Element> elements = Lists.newArrayList(
                new Entity.Builder()
                        .group("Unknown group")
                        .vertex("vertex1")
                        .property("Meaning of life", 42)
                        .build()
        );

        final Function<Element, Element> fn = new AggregatorUtil.ToIngestElementKey(schema);

        // then
        try {
            final Map<Element, List<Element>> results = elements.stream().collect(Collectors.groupingBy(fn));
        } catch (RuntimeException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldGroupElementsWithSameKey() {
        // given
        final Schema schema = createSchema();

        // when
        final List<Element> input = Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.ENTITY_2)
                        .vertex("vertex1")
                        .property(TestPropertyNames.PROP_1, "control value")
                        .property(TestPropertyNames.PROP_3, "unused")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY_2)
                        .vertex("vertex1")
                        .property(TestPropertyNames.PROP_1, "control value")
                        .property(TestPropertyNames.PROP_3, "also unused in function")
                        .build()
        );

        final Map<Element, List<Element>> results =
                input.stream().collect(
                        Collectors.groupingBy(
                                new AggregatorUtil.ToIngestElementKey(schema)
                        )
                );

        // then
        assertEquals(1, results.size());
        assertEquals(input, results.get(new Entity.Builder()
                .group(TestGroups.ENTITY_2)
                .vertex("vertex1")
                .property(TestPropertyNames.PROP_1, "control value")
                .build()));
    }

    private Schema createSchema() {
        return new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                        .property(TestPropertyNames.PROP_2, TestTypes.PROP_INTEGER)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                        .groupBy(TestPropertyNames.PROP_1)
                        .description(SchemaTest.EDGE_DESCRIPTION)
                        .validator(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                        .property(TestPropertyNames.PROP_2, TestTypes.PROP_INTEGER)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                        .groupBy(TestPropertyNames.PROP_1)
                        .description(SchemaTest.EDGE_DESCRIPTION)
                        .validator(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                        .property(TestPropertyNames.PROP_2, TestTypes.PROP_INTEGER)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                        .groupBy(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .description(SchemaTest.ENTITY_DESCRIPTION)
                        .validator(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .description(SchemaTest.STRING_TYPE_DESCRIPTION)
                        .build())
                .type(TestTypes.PROP_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .description(SchemaTest.STRING_TYPE_DESCRIPTION)
                        .build())
                .type(TestTypes.PROP_INTEGER, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .description(SchemaTest.INTEGER_TYPE_DESCRIPTION)
                        .build())
                .type(TestTypes.TIMESTAMP, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .description(SchemaTest.TIMESTAMP_TYPE_DESCRIPTION)
                        .build())
                .visibilityProperty(TestPropertyNames.VISIBILITY)
                .timestampProperty(TestPropertyNames.TIMESTAMP)
                .build();
    }
}
