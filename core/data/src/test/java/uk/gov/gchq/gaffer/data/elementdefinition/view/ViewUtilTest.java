/*
 * Copyright 2016-2021 Crown Copyright
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.function.ExampleFilterFunction;
import uk.gov.gchq.gaffer.function.ExampleTransformFunction;
import uk.gov.gchq.koryphe.impl.predicate.Exists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ViewUtilTest {

    @Test
    public void shouldNotRemovePropertiesWhenNotSet() {
        //Given
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .allProperties()
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .properties(TestPropertyNames.PROP_2)
                        .build())
                .build();

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .property(TestPropertyNames.PROP_1, "1")
                .property(TestPropertyNames.PROP_2, "2")
                .property(TestPropertyNames.PROP_3, "3")
                .build();

        //When
        ViewUtil.removeProperties(view, edge);

        //Then
        assertEquals(3, edge.getProperties().size());
        assertEquals("1", edge.getProperties().get(TestPropertyNames.PROP_1));
        assertEquals("2", edge.getProperties().get(TestPropertyNames.PROP_2));
        assertEquals("3", edge.getProperties().get(TestPropertyNames.PROP_3));
    }

    @Test
    public void shouldNotRemovePropertiesWhenNoRelevantExcludeProperties() {
        //Given
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .excludeProperties(TestPropertyNames.COUNT)
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .properties(TestPropertyNames.PROP_2)
                        .build())
                .build();

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .property(TestPropertyNames.PROP_1, "1")
                .property(TestPropertyNames.PROP_2, "2")
                .property(TestPropertyNames.PROP_3, "3")
                .build();

        //When
        ViewUtil.removeProperties(view, edge);

        //Then
        assertEquals(3, edge.getProperties().size());
        assertEquals("1", edge.getProperties().get(TestPropertyNames.PROP_1));
        assertEquals("2", edge.getProperties().get(TestPropertyNames.PROP_2));
        assertEquals("3", edge.getProperties().get(TestPropertyNames.PROP_3));
    }

    @Test
    public void shouldNotRemoveAllPropertiesWhenNoRelevantProperties() {
        //Given
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .properties(TestPropertyNames.COUNT)
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .properties(TestPropertyNames.PROP_2)
                        .build())
                .build();

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .property(TestPropertyNames.PROP_1, "1")
                .property(TestPropertyNames.PROP_2, "2")
                .property(TestPropertyNames.PROP_3, "3")
                .build();

        //When
        ViewUtil.removeProperties(view, edge);

        //Then
        assertEquals(0, edge.getProperties().size());
    }

    @Test
    public void shouldKeepOnlyProvidedProperties() {
        //Given
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .properties(TestPropertyNames.PROP_1)
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .properties(TestPropertyNames.PROP_2)
                        .build())
                .build();

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .property(TestPropertyNames.PROP_1, "1")
                .property(TestPropertyNames.PROP_2, "2")
                .property(TestPropertyNames.PROP_3, "3")
                .build();

        //When
        ViewUtil.removeProperties(view, edge);

        //Then
        assertEquals(1, edge.getProperties().size());
        assertEquals("1", edge.getProperties().get(TestPropertyNames.PROP_1));
    }

    @Test
    public void shouldRemoveExcludedProperties() {
        //Given
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .excludeProperties(TestPropertyNames.PROP_1)
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .excludeProperties(TestPropertyNames.PROP_2)
                        .build())
                .build();

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .property(TestPropertyNames.PROP_1, "1")
                .property(TestPropertyNames.PROP_2, "2")
                .property(TestPropertyNames.PROP_3, "3")
                .build();

        //When
        ViewUtil.removeProperties(view, edge);

        //Then
        assertEquals(2, edge.getProperties().size());
        assertEquals("2", edge.getProperties().get(TestPropertyNames.PROP_2));
        assertEquals("3", edge.getProperties().get(TestPropertyNames.PROP_3));
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
        final byte[] json = view.toJson(true);

        // Then
        final String expected = String.format("{%n" +
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
                "}");
        JsonAssert.assertEquals(expected, new String(json));
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() {
        // Given
        final View view = createView();

        // When
        final byte[] json = view.toJson(true);
        final View deserialisedView = new View.Builder().json(json).build();
        deserialisedView.expandGlobalDefinitions();

        // Then
        assertEquals(1, deserialisedView.getEntityGroups().size());
        final ViewElementDefinition entityDef = deserialisedView.getEntity(TestGroups.ENTITY);
        assertTrue(entityDef.getTransientProperties().isEmpty());
        assertNull(entityDef.getTransformer());
        assertEquals(2, entityDef.getPreAggregationFilter().getComponents().size());
        assertTrue(entityDef.getPreAggregationFilter().getComponents().get(0).getPredicate() instanceof ExampleFilterFunction);
        assertThat(entityDef.getPreAggregationFilter().getComponents().get(0).getSelection()).hasSize(1);
        assertEquals(TestPropertyNames.PROP_1, entityDef.getPreAggregationFilter().getComponents().get(0).getSelection()[0]);
        assertEquals(TestPropertyNames.PROP_1, entityDef.getPreAggregationFilter().getComponents().get(1).getSelection()[0]);
        assertThat(entityDef.getPostAggregationFilter().getComponents().get(0).getSelection()).hasSize(1);
        assertEquals(IdentifierType.VERTEX.name(), entityDef.getPostAggregationFilter().getComponents().get(0).getSelection()[0]);

        final ViewElementDefinition edgeDef = deserialisedView.getEdge(TestGroups.EDGE);
        assertEquals(1, edgeDef.getTransientProperties().size());
        assertEquals(String.class, edgeDef.getTransientPropertyMap().get(TestPropertyNames.PROP_3));
        assertEquals(1, edgeDef.getPreAggregationFilter().getComponents().size());
        assertTrue(edgeDef.getPreAggregationFilter().getComponents().get(0).getPredicate() instanceof ExampleFilterFunction);
        assertThat(edgeDef.getPreAggregationFilter().getComponents().get(0).getSelection()).hasSize(1);
        assertEquals(TestPropertyNames.PROP_1, edgeDef.getPreAggregationFilter().getComponents().get(0).getSelection()[0]);
        assertEquals(1, edgeDef.getTransformer().getComponents().size());
        assertTrue(edgeDef.getTransformer().getComponents().get(0).getFunction() instanceof ExampleTransformFunction);
        assertThat(edgeDef.getTransformer().getComponents().get(0).getSelection()).hasSize(2);
        assertEquals(TestPropertyNames.PROP_1, edgeDef.getTransformer().getComponents().get(0).getSelection()[0]);
        assertEquals(TestPropertyNames.PROP_2, edgeDef.getTransformer().getComponents().get(0).getSelection()[1]);
        assertThat(edgeDef.getTransformer().getComponents().get(0).getProjection()).hasSize(1);
        assertEquals(TestPropertyNames.PROP_3, edgeDef.getTransformer().getComponents().get(0).getProjection()[0]);
        assertEquals(1, edgeDef.getPostTransformFilter().getComponents().size());
        assertTrue(edgeDef.getPostTransformFilter().getComponents().get(0).getPredicate() instanceof ExampleFilterFunction);
        assertThat(edgeDef.getPostTransformFilter().getComponents().get(0).getSelection()).hasSize(1);
        assertEquals(TestPropertyNames.PROP_3, edgeDef.getPostTransformFilter().getComponents().get(0).getSelection()[0]);
        assertThat(edgeDef.getPostAggregationFilter().getComponents().get(0).getSelection()).hasSize(1);
        assertEquals(IdentifierType.SOURCE.name(), edgeDef.getPostAggregationFilter().getComponents().get(0).getSelection()[0]);
    }

    @Test
    public void shouldCreateViewWithGlobalDefinitions() {
        // Given
        final View view = new View.Builder()
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

        // When
        view.expandGlobalDefinitions();

        final String expected = String.format("{%n" +
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
                "}");
        JsonAssert.assertEquals(expected, new String(view.toJson(true)));
    }

    @Test
    public void shouldCreateAnIdenticalObjectWhenCloned() throws IOException{
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

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode treeView = objectMapper.readTree(viewJson);
        JsonNode treeClone = objectMapper.readTree(cloneJson);

        // Check that JSON representations of the objects are equal
        assertThat(treeView).isEqualTo(treeClone);


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
    public void shouldReturnTrueWhenViewHasPreAggEntityFilters() {
        // Given
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new Exists())
                                .build())
                        .build())
                .edge(TestGroups.EDGE)
                .edge(TestGroups.EDGE_2, null)
                .build();

        // When
        final boolean result = view.hasPreAggregationFilters();

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldReturnTrueWhenViewHasPreAggEdgeFilters() {
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY)
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new Exists())
                                .build())
                        .build())
                .edge(TestGroups.EDGE_2, null)
                .build();

        final boolean result = view.hasPreAggregationFilters();

        assertTrue(result);
    }

    @Test
    public void shouldReturnFalseWhenViewHasNullPreAggEdgeFilters() {
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .preAggregationFilter(null)
                        .build())
                .build();

        final boolean result = view.hasPreAggregationFilters();

        assertFalse(result);
    }

    @Test
    public void shouldReturnFalseWhenViewHasEmptyPreAggEdgeFilters() {
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .build())
                        .build())
                .build();

        final boolean result = view.hasPreAggregationFilters();

        assertFalse(result);
    }

    @Test
    public void shouldReturnTrueWhenViewHasPostAggEntityFilters() {
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .postAggregationFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new Exists())
                                .build())
                        .build())
                .edge(TestGroups.EDGE)
                .edge(TestGroups.EDGE_2, null)
                .build();

        final boolean result = view.hasPostAggregationFilters();

        assertTrue(result);
    }

    @Test
    public void shouldReturnTrueWhenViewHasPostAggEdgeFilters() {
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY)
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .postAggregationFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new Exists())
                                .build())
                        .build())
                .edge(TestGroups.EDGE_2, null)
                .build();

        final boolean result = view.hasPostAggregationFilters();

        assertTrue(result);
    }

    @Test
    public void shouldReturnFalseWhenViewHasNullPostAggEdgeFilters() {
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .postAggregationFilter(null)
                        .build())
                .build();

        final boolean result = view.hasPostAggregationFilters();

        assertFalse(result);
    }

    @Test
    public void shouldReturnFalseWhenViewHasEmptyPostAggEdgeFilters() {
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .postAggregationFilter(new ElementFilter.Builder()
                                .build())
                        .build())
                .build();

        final boolean result = view.hasPostAggregationFilters();

        assertFalse(result);
    }

    @Test
    public void shouldReturnTrueWhenViewHasPostTransformEntityFilters() {
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .postTransformFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new Exists())
                                .build())
                        .build())
                .edge(TestGroups.EDGE)
                .edge(TestGroups.EDGE_2, null)
                .build();

        final boolean result = view.hasPostTransformFilters();

        assertTrue(result);
    }

    @Test
    public void shouldReturnTrueWhenViewHasPostTransformEdgeFilters() {
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY)
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .postTransformFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new Exists())
                                .build())
                        .build())
                .edge(TestGroups.EDGE_2, null)
                .build();

        final boolean result = view.hasPostTransformFilters();

        assertTrue(result);
    }

    @Test
    public void shouldReturnFalseWhenViewHasNullPostTransformEdgeFilters() {
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .postTransformFilter(null)
                        .build())
                .build();

        final boolean result = view.hasPostTransformFilters();

        assertFalse(result);
    }

    @Test
    public void shouldReturnFalseWhenViewHasEmptyPostTransformEdgeFilters() {
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .postTransformFilter(new ElementFilter.Builder()
                                .build())
                        .build())
                .build();

        final boolean result = view.hasPostTransformFilters();

        assertFalse(result);
    }

    @Test
    public void shouldRemoveGroupFromView() {
        View view = new View.Builder()
                .edge(TestGroups.EDGE)
                .entity(TestGroups.ENTITY)
                .build();

        // Then
        assertEquals(2, view.getGroups().size());

        // When
        view = ViewUtil.removeGroups(view, TestGroups.EDGE);

        // Then
        assertFalse(view.getGroups().contains(TestGroups.EDGE));
        assertEquals(1, view.getGroups().size());

        // When
        view = ViewUtil.removeGroups(view, TestGroups.ENTITY);

        // Then
        assertFalse(view.getGroups().contains(TestGroups.ENTITY));
        assertEquals(0, view.getGroups().size());
    }

    @Test
    public void shouldIgnoreRemovingGroupFromViewWhenNotSet() {
        final View view = new View.Builder()
                .edge(TestGroups.EDGE)
                .build();

        final View viewAfterRemove = ViewUtil.removeGroups(view, TestGroups.ENTITY);

        JsonAssert.assertEquals(view.toJson(false), viewAfterRemove.toJson(false));
    }

    @Test
    public void shouldThrowExceptionOnRemovalOfNullGroups() {
        final View view = new View.Builder()
                .edge(TestGroups.EDGE)
                .build();

        assertThatIllegalArgumentException()
                .isThrownBy(() -> ViewUtil.removeGroups(view, null))
                .withMessage("Specified group(s) to remove is null");
    }

    @Test
    public void shouldThrowExceptionOnWhenRemovingGroupFromNullView() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ViewUtil.removeGroups(null, TestGroups.EDGE))
                .withMessage("View cannot be null");
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
