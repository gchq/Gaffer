/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.data.elementdefinition.view;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.function.ExampleFilterFunction;
import uk.gov.gchq.gaffer.function.ExampleTransformFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ViewTest {

    @Test
    public void shouldCreateEmptyViewWithBasicConstructor() {
        //Given

        //When
        View view = new View();

        //Then
        assertTrue(view.getEdges().isEmpty());
        assertTrue(view.getEntities().isEmpty());
    }

    @Test
    public void shouldCreateNewViewWithEdgeAndEntityGroups() {
        //Given
        List<String> entityGroups = new ArrayList<>();
        List<String> edgeGroups = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            entityGroups.add(TestGroups.ENTITY + i);
            edgeGroups.add(TestGroups.EDGE + i);
        }

        //When
        View view = new View.Builder()
                .entities(entityGroups)
                .edges(edgeGroups)
                .build();

        //Then
        assertTrue(view.getEntityGroups().containsAll(entityGroups));
        assertEquals(entityGroups.size(), view.getEntityGroups().size());
        assertTrue(view.getEdgeGroups().containsAll(edgeGroups));
        assertEquals(edgeGroups.size(), view.getEdgeGroups().size());
    }

    @Test
    public void shouldBuildView() {
        // Given
        final ViewElementDefinition edgeDef1 = new ViewElementDefinition();
        final ViewElementDefinition edgeDef2 = new ViewElementDefinition();
        final ViewElementDefinition entityDef1 = new ViewElementDefinition();
        final ViewElementDefinition entityDef2 = new ViewElementDefinition();

        // When
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, edgeDef1)
                .entity(TestGroups.ENTITY, entityDef1)
                .entity(TestGroups.ENTITY_2, entityDef2)
                .edge(TestGroups.EDGE_2, edgeDef2)
                .build();

        // Then
        assertEquals(2, view.getEdges().size());
        assertSame(edgeDef1, view.getEdge(TestGroups.EDGE));
        assertSame(edgeDef2, view.getEdge(TestGroups.EDGE_2));

        assertEquals(2, view.getEntities().size());
        assertSame(entityDef1, view.getEntity(TestGroups.ENTITY));
        assertSame(entityDef2, view.getEntity(TestGroups.ENTITY_2));
    }

    @Test
    public void shouldSerialiseToJson() {
        // Given
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .transientProperty(TestPropertyNames.PROP_3, String.class)
                        .transformer(new ElementTransformer.Builder()
                                .select(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                                .execute(new ExampleTransformFunction())
                                .project(TestPropertyNames.PROP_3)
                                .build())
                        .postTransformFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_3)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .build();

        // When
        byte[] json = view.toJson(true);

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
            "  \"edges\" : {%n" +
            "    \"BasicEdge\" : {%n" +
            "      \"transientProperties\" : {%n" +
            "        \"property3\" : \"java.lang.String\"%n" +
            "      },%n" +
            "      \"postTransformFilterFunctions\" : [ {%n" +
            "        \"predicate\" : {%n" +
            "          \"class\" : \"uk.gov.gchq.gaffer.function.ExampleFilterFunction\"%n" +
            "        },%n" +
            "        \"selection\" : [ \"property3\" ]%n" +
            "      } ],%n" +
            "      \"transformFunctions\" : [ {%n" +
            "        \"function\" : {%n" +
            "          \"class\" : \"uk.gov.gchq.gaffer.function.ExampleTransformFunction\"%n" +
            "        },%n" +
            "        \"selection\" : [ \"property1\", \"property2\" ],%n" +
            "        \"projection\" : [ \"property3\" ]%n" +
            "      } ]%n" +
            "    }%n" +
            "  },%n" +
            "  \"entities\" : {%n" +
            "    \"BasicEntity\" : {%n" +
            "      \"preAggregationFilterFunctions\" : [ {%n" +
            "        \"predicate\" : {%n" +
            "          \"class\" : \"uk.gov.gchq.gaffer.function.ExampleFilterFunction\"%n" +
            "        },%n" +
            "        \"selection\" : [ \"property1\" ]%n" +
            "      } ]%n" +
            "    }%n" +
            "  }%n" +
            "}"), new String(json));
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() {
        // Given
        final View view = createView();

        // When
        byte[] json = view.toJson(true);
        final View deserialisedView = new View.Builder().json(json).build();
        deserialisedView.expandGlobalDefinitions();

        // Then
        assertEquals(1, deserialisedView.getEntityGroups().size());
        final ViewElementDefinition entityDef = deserialisedView.getEntity(TestGroups.ENTITY);
        assertTrue(entityDef.getTransientProperties().isEmpty());
        assertNull(entityDef.getTransformer());
        assertEquals(2, entityDef.getPreAggregationFilter().getComponents().size());
        assertTrue(entityDef.getPreAggregationFilter().getComponents().get(0).getPredicate() instanceof ExampleFilterFunction);
        assertEquals(1, entityDef.getPreAggregationFilter().getComponents().get(0).getSelection().length);
        assertEquals(TestPropertyNames.PROP_1, entityDef.getPreAggregationFilter().getComponents().get(0).getSelection()[0]);
        assertEquals(TestPropertyNames.PROP_1, entityDef.getPreAggregationFilter().getComponents().get(1).getSelection()[0]);
        assertEquals(1, entityDef.getPostAggregationFilter().getComponents().get(0).getSelection().length);
        assertEquals(IdentifierType.VERTEX.name(), entityDef.getPostAggregationFilter().getComponents().get(0).getSelection()[0]);

        final ViewElementDefinition edgeDef = deserialisedView.getEdge(TestGroups.EDGE);
        assertEquals(1, edgeDef.getTransientProperties().size());
        assertEquals(String.class, edgeDef.getTransientPropertyMap().get(TestPropertyNames.PROP_3));
        assertEquals(1, edgeDef.getPreAggregationFilter().getComponents().size());
        assertTrue(edgeDef.getPreAggregationFilter().getComponents().get(0).getPredicate() instanceof ExampleFilterFunction);
        assertEquals(1, edgeDef.getPreAggregationFilter().getComponents().get(0).getSelection().length);
        assertEquals(TestPropertyNames.PROP_1, edgeDef.getPreAggregationFilter().getComponents().get(0).getSelection()[0]);
        assertEquals(1, edgeDef.getTransformer().getComponents().size());
        assertTrue(edgeDef.getTransformer().getComponents().get(0).getFunction() instanceof ExampleTransformFunction);
        assertEquals(2, edgeDef.getTransformer().getComponents().get(0).getSelection().length);
        assertEquals(TestPropertyNames.PROP_1, edgeDef.getTransformer().getComponents().get(0).getSelection()[0]);
        assertEquals(TestPropertyNames.PROP_2, edgeDef.getTransformer().getComponents().get(0).getSelection()[1]);
        assertEquals(1, edgeDef.getTransformer().getComponents().get(0).getProjection().length);
        assertEquals(TestPropertyNames.PROP_3, edgeDef.getTransformer().getComponents().get(0).getProjection()[0]);
        assertEquals(1, edgeDef.getPostTransformFilter().getComponents().size());
        assertTrue(edgeDef.getPostTransformFilter().getComponents().get(0).getPredicate() instanceof ExampleFilterFunction);
        assertEquals(1, edgeDef.getPostTransformFilter().getComponents().get(0).getSelection().length);
        assertEquals(TestPropertyNames.PROP_3, edgeDef.getPostTransformFilter().getComponents().get(0).getSelection()[0]);
        assertEquals(1, edgeDef.getPostAggregationFilter().getComponents().get(0).getSelection().length);
        assertEquals(IdentifierType.SOURCE.name(), edgeDef.getPostAggregationFilter().getComponents().get(0).getSelection()[0]);
    }

    @Test
    public void shouldCreateViewWithGlobalDefinitions() {
        // Given
        final View view = createViewWithGroupByFields();

        // When
        view.expandGlobalDefinitions();

        JsonUtil.assertEquals(String.format("{%n" +
            "  \"edges\" : {%n" +
            "    \"BasicEdge2\" : {%n" +
            "      \"groupBy\" : [ ],%n" +
            "      \"transientProperties\" : {%n" +
            "        \"property2\" : \"java.lang.String\"%n" +
            "      },%n" +
            "      \"preAggregationFilterFunctions\" : [ {%n" +
            "        \"predicate\" : {%n" +
            "          \"class\" : \"uk.gov.gchq.gaffer.function.ExampleFilterFunction\"%n" +
            "        },%n" +
            "        \"selection\" : [ \"property1\" ]%n" +
            "      } ],%n" +
            "      \"postTransformFilterFunctions\" : [ {%n" +
            "        \"predicate\" : {%n" +
            "          \"class\" : \"uk.gov.gchq.gaffer.function.ExampleFilterFunction\"%n" +
            "        },%n" +
            "        \"selection\" : [ \"SOURCE\" ]%n" +
            "      } ]%n" +
            "    },%n" +
            "    \"BasicEdge\" : {%n" +
            "      \"groupBy\" : [ ],%n" +
            "      \"transientProperties\" : {%n" +
            "        \"property2\" : \"java.lang.String\"%n" +
            "      },%n" +
            "      \"preAggregationFilterFunctions\" : [ {%n" +
            "        \"predicate\" : {%n" +
            "          \"class\" : \"uk.gov.gchq.gaffer.function.ExampleFilterFunction\"%n" +
            "        },%n" +
            "        \"selection\" : [ \"property1\" ]%n" +
            "      } ],%n" +
            "      \"postTransformFilterFunctions\" : [ {%n" +
            "        \"predicate\" : {%n" +
            "          \"class\" : \"uk.gov.gchq.gaffer.function.ExampleFilterFunction\"%n" +
            "        },%n" +
            "        \"selection\" : [ \"SOURCE\" ]%n" +
            "      } ]%n" +
            "    },%n" +
            "    \"BasicEdge3\" : {%n" +
            "      \"groupBy\" : [ \"property1\" ],%n" +
            "      \"transientProperties\" : {%n" +
            "        \"property2\" : \"java.lang.String\"%n" +
            "      },%n" +
            "      \"preAggregationFilterFunctions\" : [ {%n" +
            "        \"predicate\" : {%n" +
            "          \"class\" : \"uk.gov.gchq.gaffer.function.ExampleFilterFunction\"%n" +
            "        },%n" +
            "        \"selection\" : [ \"property1\" ]%n" +
            "      } ]%n" +
            "    }%n" +
            "  },%n" +
            "  \"entities\" : {%n" +
            "    \"BasicEntity2\" : {%n" +
            "      \"groupBy\" : [ \"property1\" ],%n" +
            "      \"transientProperties\" : {%n" +
            "        \"property2\" : \"java.lang.String\"%n" +
            "      },%n" +
            "      \"preAggregationFilterFunctions\" : [ {%n" +
            "        \"predicate\" : {%n" +
            "          \"class\" : \"uk.gov.gchq.gaffer.function.ExampleFilterFunction\"%n" +
            "        },%n" +
            "        \"selection\" : [ \"property1\" ]%n" +
            "      } ],%n" +
            "      \"postAggregationFilterFunctions\" : [ {%n" +
            "        \"predicate\" : {%n" +
            "          \"class\" : \"uk.gov.gchq.gaffer.function.ExampleFilterFunction\"%n" +
            "        },%n" +
            "        \"selection\" : [ \"VERTEX\" ]%n" +
            "      } ]%n" +
            "    },%n" +
            "    \"BasicEntity\" : {%n" +
            "      \"groupBy\" : [ \"property1\" ],%n" +
            "      \"transientProperties\" : {%n" +
            "        \"property2\" : \"java.lang.String\"%n" +
            "      },%n" +
            "      \"preAggregationFilterFunctions\" : [ {%n" +
            "        \"predicate\" : {%n" +
            "          \"class\" : \"uk.gov.gchq.gaffer.function.ExampleFilterFunction\"%n" +
            "        },%n" +
            "        \"selection\" : [ \"property1\" ]%n" +
            "      } ],%n" +
            "      \"postAggregationFilterFunctions\" : [ {%n" +
            "        \"predicate\" : {%n" +
            "          \"class\" : \"uk.gov.gchq.gaffer.function.ExampleFilterFunction\"%n" +
            "        },%n" +
            "        \"selection\" : [ \"VERTEX\" ]%n" +
            "      } ]%n" +
            "    },%n" +
            "    \"BasicEntity3\" : {%n" +
            "      \"groupBy\" : [ \"dateProperty\" ],%n" +
            "      \"transientProperties\" : {%n" +
            "        \"property2\" : \"java.lang.String\"%n" +
            "      },%n" +
            "      \"preAggregationFilterFunctions\" : [ {%n" +
            "        \"predicate\" : {%n" +
            "          \"class\" : \"uk.gov.gchq.gaffer.function.ExampleFilterFunction\"%n" +
            "        },%n" +
            "        \"selection\" : [ \"property1\" ]%n" +
            "      }, {%n" +
            "        \"predicate\" : {%n" +
            "          \"class\" : \"uk.gov.gchq.gaffer.function.ExampleFilterFunction\"%n" +
            "        },%n" +
            "        \"selection\" : [ \"dateProperty\" ]%n" +
            "      } ]%n" +
            "    }%n" +
            "  }%n" +
            "}"), new String(view.toJson(true)));
    }

    @Test
    public void shouldCreateAnIdenticalObjectWhenCloned() {
        // Given
        final ViewElementDefinition edgeDef1 = new ViewElementDefinition();
        final ViewElementDefinition edgeDef2 = new ViewElementDefinition();
        final ViewElementDefinition entityDef1 = new ViewElementDefinition();
        final ViewElementDefinition entityDef2 = new ViewElementDefinition();

        // When
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, edgeDef1)
                .entity(TestGroups.ENTITY, entityDef1)
                .entity(TestGroups.ENTITY_2, entityDef2)
                .edge(TestGroups.EDGE_2, edgeDef2)
                .build();

        // Then
        final View clone = view.clone();

        // Check that the objects are equal
        assertEquals(view, clone);

        final byte[] viewJson = view.toCompactJson();
        final byte[] cloneJson = clone.toCompactJson();

        // Check that JSON representations of the objects are equal
        assertArrayEquals(viewJson, cloneJson);

        final View viewFromJson = new View.Builder().json(viewJson).build();
        final View cloneFromJson = new View.Builder().json(cloneJson).build();

        // Check that objects created from JSON representations are equal
        assertEquals(viewFromJson, cloneFromJson);

        // Check that objects created from JSON representations are equal
        assertEquals(viewFromJson, view);
        assertEquals(cloneFromJson, clone);
    }

    @Test
    public void shouldSerialiseToCompactJson() {
        // Given
        final View view = new View();

        // When
        final String compactJson = new String(view.toCompactJson());

        // Then - no description fields or new lines
        assertFalse(compactJson.contains(String.format("%n")));
    }

    @Test
    public void shouldMergeDifferentViews() {
        // Given
        final View view1 = new View.Builder()
                .entity(TestGroups.ENTITY)
                .edge(TestGroups.EDGE)
                .build();

        final View view2 = new View.Builder()
                .entity(TestGroups.ENTITY)
                .entity(TestGroups.ENTITY_2)
                .edge(TestGroups.EDGE)
                .edge(TestGroups.EDGE_2)
                .build();

        // When
        final View mergedView = new View.Builder()
                .merge(view1)
                .merge(view2)
                .build();

        // Then
        assertEquals(2, mergedView.getEntities().size());
        assertEquals(2, mergedView.getEdges().size());
    }

    @Test
    public void shouldGetAllGroups() {
        // Given
        final View view = createView();

        // When
        final Set<String> groups = view.getGroups();

        // Then
        final Set<String> allGroups = new HashSet<>(view.getEntityGroups());
        allGroups.addAll(view.getEdgeGroups());

        assertEquals(allGroups, groups);
    }

    @Test
    public void shouldCollectAllElementsTogetherIfNoGroupByIsStated() {
        // given
        View view = createView();
        view.expandGlobalDefinitions();

        // when
        Function<Element, Set<Object>> fn = view.createGroupByFunction();

        List<Element> input = Arrays.asList(
            new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("vertex1")
                .build(),
            new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("vertex2")
                .build()
        );
        // then

        Map<Set<Object>, List<Element>> results = input.stream().collect(Collectors.groupingBy(fn));
        Map<Set<Object>, List<Element>> expected = new HashMap<>();
        expected.put(Sets.newHashSet("vertex1", TestGroups.ENTITY), Lists.newArrayList(input.get(0)));
        expected.put(Sets.newHashSet("vertex2", TestGroups.ENTITY), Lists.newArrayList(input.get(1)));

        assertEquals(expected, results);
    }

    @Test
    public void shouldCollectElementsTogetherIfGroupByIsSpecifiedInTheGlobalElementDefinitions() {
        // given
        View view = createViewWithGroupByFields();
        // safe to assume global edges are expanded
        view.expandGlobalDefinitions();
        // when
        Function<Element, Set<Object>> fn = view.createGroupByFunction();
        List<Element> input = Arrays.asList(
            new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("vertex1")
                .property(TestPropertyNames.PROP_1, "test1")
                .build(),
            new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("vertex2")
                .property(TestPropertyNames.PROP_1, "test2")
                .build(),
            new Entity.Builder()
                .group(TestGroups.ENTITY_2)
                .vertex("vertex3")
                .property(TestPropertyNames.PROP_1, "test2")
                .build(),
            new Edge.Builder()  // group by is overwritten by global Edges
                .group(TestGroups.EDGE)
                .source("vertex1")
                .dest("vertex2")
                .property(TestPropertyNames.PROP_1, "test2")
                .build()
        );

        // then

        Map<Set<Object>, List<Element>> results = input.stream().collect(Collectors.groupingBy(fn));
        Map<Set<Object>, List<Element>> expected = new HashMap<>();
        expected.put(Sets.newHashSet(DirectedType.UNDIRECTED, "vertex1", "vertex2", TestGroups.EDGE), Collections.singletonList(input.get(3)));
        expected.put(Sets.newHashSet("test1", "vertex1", TestGroups.ENTITY), Collections.singletonList(input.get(0)));
        expected.put(Sets.newHashSet("test2", "vertex2", TestGroups.ENTITY), Lists.newArrayList(input.get(1)));
        expected.put(Sets.newHashSet("test2", "vertex3", TestGroups.ENTITY_2), Lists.newArrayList(input.get(2)));

        assertEquals(expected, results);

    }

    @Test
    public void shouldCollectElementsTogetherWhenGroupByIsDefinedByViewDefinition() {
        // given
        View view = createViewWithGroupByFields();
        view.expandGlobalDefinitions();

        // when

        Function<Element, Set<Object>> fn = view.createGroupByFunction();
        List<Element> input = Arrays.asList(
            new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex("vertex1")
                .property(TestPropertyNames.DATE, "10-03-2017")
                .build(),
            new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex("vertex2")
                .property(TestPropertyNames.DATE, "10-03-2017")
                .property(TestPropertyNames.PROP_1, "test1")
                .build(),
            new Entity.Builder()
                .group(TestGroups.ENTITY_2)
                .vertex("vertex2")
                .property(TestPropertyNames.DATE, "10-03-2017")
                .property(TestPropertyNames.PROP_1, "test1")
                .build(),
            new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex("vertex2")
                .property(TestPropertyNames.DATE, "21-06-2016")
                .property(TestPropertyNames.PROP_1, "test2")
                .build()
        );

        // then

        Map<Set<Object>, List<Element>> results = input.stream().collect(Collectors.groupingBy(fn));
        Map<Set<Object>, List<Element>> expected = new HashMap<>();
        expected.put(Sets.newHashSet("10-03-2017", "vertex1", TestGroups.ENTITY_3), Lists.newArrayList(input.get(0)));
        expected.put(Sets.newHashSet("10-03-2017", "vertex2", TestGroups.ENTITY_3), Lists.newArrayList(input.get(1)));
        expected.put(Sets.newHashSet("test1", "vertex2", TestGroups.ENTITY_2), Lists.newArrayList(input.get(2)));
        expected.put(Sets.newHashSet("21-06-2016", "vertex2",TestGroups.ENTITY_3), Lists.newArrayList(input.get(3)));


        assertEquals(expected, results);

    }

    private View createViewWithGroupByFields() {
        return new View.Builder()
            .globalElements(new GlobalViewElementDefinition.Builder()
                .preAggregationFilter(new ElementFilter.Builder()
                    .select(TestPropertyNames.PROP_1)
                    .execute(new ExampleFilterFunction())
                    .build())
                .groupBy(TestPropertyNames.PROP_1)
                .transientProperty(TestPropertyNames.PROP_2, String.class)
                .build())
            .globalEntities(new GlobalViewElementDefinition.Builder()
                .postAggregationFilter(new ElementFilter.Builder()
                    .select(IdentifierType.VERTEX.name())
                    .execute(new ExampleFilterFunction())
                    .build())
                .groups(TestGroups.ENTITY, TestGroups.ENTITY_2)
                .build())
            .globalEdges(new GlobalViewElementDefinition.Builder()
                .postTransformFilter(new ElementFilter.Builder()
                    .select(IdentifierType.SOURCE.name())
                    .execute(new ExampleFilterFunction())
                    .build())
                .groupBy()
                .groups(TestGroups.EDGE, TestGroups.EDGE_2)
                .build())
            .entity(TestGroups.ENTITY_3, new ViewElementDefinition.Builder()
                .preAggregationFilter(new ElementFilter.Builder()
                    .select(TestPropertyNames.DATE)
                    .execute(new ExampleFilterFunction())
                    .build())
                .groupBy(TestPropertyNames.DATE)
                .build())
            .entity(TestGroups.ENTITY)
            .entity(TestGroups.ENTITY_2)
            .edge(TestGroups.EDGE)
            .edge(TestGroups.EDGE_2)
            .edge(TestGroups.EDGE_3)
            .build();
    }
    private View createView() {
        return new View.Builder()
                .globalElements(new GlobalViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .globalEntities(new GlobalViewElementDefinition.Builder()
                        .postAggregationFilter(new ElementFilter.Builder()
                                .select(IdentifierType.VERTEX.name())
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .globalEdges(new GlobalViewElementDefinition.Builder()
                        .postAggregationFilter(new ElementFilter.Builder()
                                .select(IdentifierType.SOURCE.name())
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .transientProperty(TestPropertyNames.PROP_3, String.class)
                        .transformer(new ElementTransformer.Builder()
                                .select(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                                .execute(new ExampleTransformFunction())
                                .project(TestPropertyNames.PROP_3)
                                .build())
                        .postTransformFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_3)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .build();
    }
}
