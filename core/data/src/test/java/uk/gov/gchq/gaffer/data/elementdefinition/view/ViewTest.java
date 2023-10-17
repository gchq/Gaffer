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

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.function.ExampleFilterFunction;
import uk.gov.gchq.gaffer.function.ExampleTransformFunction;
import uk.gov.gchq.koryphe.impl.function.Identity;
import uk.gov.gchq.koryphe.impl.predicate.Exists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class ViewTest extends JSONSerialisationTest<View> {

    @Test
    public void shouldCreateEmptyViewWithBasicConstructor() {
        final View view = new View();

        assertThat(view.getEdges().isEmpty()).isTrue();
        assertThat(view.getEntities().isEmpty()).isTrue();
    }

    @Test
    public void shouldCreateNewViewWithEdgeAndEntityGroups() {
        final List<String> entityGroups = new ArrayList<>();
        final List<String> edgeGroups = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            entityGroups.add(TestGroups.ENTITY + i);
            edgeGroups.add(TestGroups.EDGE + i);
        }

        //When
        final View view = new View.Builder()
                .entities(entityGroups)
                .edges(edgeGroups)
                .build();

        //Then
        assertThat(view.getEntityGroups().containsAll(entityGroups)).isTrue();
        assertThat(view.getEntityGroups()).hasSize(entityGroups.size());
        assertThat(view.getEdgeGroups().containsAll(edgeGroups)).isTrue();
        assertThat(view.getEdgeGroups()).hasSize(edgeGroups.size());
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
        assertThat(view.getEdges()).hasSize(2);
        assertThat(view.getEdge(TestGroups.EDGE)).isSameAs(edgeDef1);
        assertThat(view.getEdge(TestGroups.EDGE_2)).isSameAs(edgeDef2);

        assertThat(view.getEntities()).hasSize(2);
        assertThat(view.getEntity(TestGroups.ENTITY)).isSameAs(entityDef1);
        assertThat(view.getEntity(TestGroups.ENTITY_2)).isSameAs(entityDef2);
    }

    @Test
    public void shouldSerialiseToJsonSkippingEmptyElementMaps() {
        final View view = new View.Builder()
                .globalEdges(new GlobalViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();

        final byte[] json = toJson(view);

        final String expected = String.format("{" +
                "  \"globalEdges\" : [ {%n" +
                "    \"groupBy\" : [ ]%n" +
                "  } ]%n" +
                "}");
        JsonAssert.assertEquals(expected, new String(json));
    }

    @Test
    public void shouldSerialiseToJson() {
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
                .config("key1", "value1")
                .build();

        final byte[] json = view.toJson(true);

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
                "  },%n" +
                " \"config\" : { \"key1\": \"value1\"}" +
                "}");
        JsonAssert.assertEquals(expected, new String(json));
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
        assertThat(deserialisedView.getEntityGroups()).hasSize(1);
        final ViewElementDefinition entityDef = deserialisedView.getEntity(TestGroups.ENTITY);
        assertThat(entityDef.getTransientProperties().isEmpty()).isTrue();
        assertThat(entityDef.getTransformer()).isNull();
        assertThat(entityDef.getPreAggregationFilter().getComponents()).hasSize(2);
        assertThat(entityDef.getPreAggregationFilter().getComponents().get(0).getPredicate()).isInstanceOf(ExampleFilterFunction.class);
        assertThat(entityDef.getPreAggregationFilter().getComponents().get(0).getSelection()).hasSize(1);
        assertThat(entityDef.getPreAggregationFilter().getComponents().get(0).getSelection()[0]).isEqualTo(TestPropertyNames.PROP_1);
        assertThat(entityDef.getPreAggregationFilter().getComponents().get(1).getSelection()[0]).isEqualTo(TestPropertyNames.PROP_1);
        assertThat(entityDef.getPostAggregationFilter().getComponents().get(0).getSelection()).hasSize(1);
        assertThat(entityDef.getPostAggregationFilter().getComponents().get(0).getSelection()[0]).isEqualTo(IdentifierType.VERTEX.name());

        final ViewElementDefinition edgeDef = deserialisedView.getEdge(TestGroups.EDGE);
        assertThat(edgeDef.getTransientProperties()).hasSize(1);
        assertThat(edgeDef.getTransientPropertyMap().get(TestPropertyNames.PROP_3)).isEqualTo(String.class);
        assertThat(edgeDef.getPreAggregationFilter().getComponents()).hasSize(1);
        assertThat(edgeDef.getPreAggregationFilter().getComponents().get(0).getPredicate()).isInstanceOf(ExampleFilterFunction.class);
        assertThat(edgeDef.getPreAggregationFilter().getComponents().get(0).getSelection()).hasSize(1);
        assertThat(edgeDef.getPreAggregationFilter().getComponents().get(0).getSelection()[0]).isEqualTo(TestPropertyNames.PROP_1);
        assertThat(edgeDef.getTransformer().getComponents()).hasSize(1);
        assertThat(edgeDef.getTransformer().getComponents().get(0).getFunction()).isInstanceOf(ExampleTransformFunction.class);
        assertThat(edgeDef.getTransformer().getComponents().get(0).getSelection()).hasSize(2);
        assertThat(edgeDef.getTransformer().getComponents().get(0).getSelection()[0]).isEqualTo(TestPropertyNames.PROP_1);
        assertThat(edgeDef.getTransformer().getComponents().get(0).getSelection()[1]).isEqualTo(TestPropertyNames.PROP_2);
        assertThat(edgeDef.getTransformer().getComponents().get(0).getProjection()).hasSize(1);
        assertThat(edgeDef.getTransformer().getComponents().get(0).getProjection()[0]).isEqualTo(TestPropertyNames.PROP_3);
        assertThat(edgeDef.getPostTransformFilter().getComponents()).hasSize(1);
        assertThat(edgeDef.getPostTransformFilter().getComponents().get(0).getPredicate()).isInstanceOf(ExampleFilterFunction.class);
        assertThat(edgeDef.getPostTransformFilter().getComponents().get(0).getSelection()).hasSize(1);
        assertThat(edgeDef.getPostTransformFilter().getComponents().get(0).getSelection()[0]).isEqualTo(TestPropertyNames.PROP_3);
        assertThat(edgeDef.getPostAggregationFilter().getComponents().get(0).getSelection()).hasSize(1);
        assertThat(edgeDef.getPostAggregationFilter().getComponents().get(0).getSelection()[0]).isEqualTo(IdentifierType.SOURCE.name());

        assertThat(deserialisedView.getConfig()).isEqualTo(view.getConfig());
        assertThat(deserialisedView.getConfig().get("key1")).isEqualTo("value1");
    }

    @Override
    protected View getTestObject() {
        return new View();
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

        JsonAssert.assertEquals(String.format("{%n" +
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
                "  } %n" +
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
        assertThat(clone).isEqualTo(view);

        final byte[] viewJson = view.toCompactJson();
        final byte[] cloneJson = clone.toCompactJson();

        // Check that JSON representations of the objects are equal
        assertThat(cloneJson).containsExactly(viewJson);

        final View viewFromJson = new View.Builder().json(viewJson).build();
        final View cloneFromJson = new View.Builder().json(cloneJson).build();

        // Check that objects created from JSON representations are equal
        assertThat(cloneFromJson).isEqualTo(viewFromJson);

        // Check that objects created from JSON representations are equal
        assertThat(view).isEqualTo(viewFromJson);
        assertThat(clone).isEqualTo(cloneFromJson);
    }

    @Test
    public void shouldSerialiseToCompactJson() {
        final View view = new View();

        final String compactJson = new String(view.toCompactJson());

        assertThat(compactJson.contains(String.format("%n"))).isFalse();
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
        assertThat(mergedView.getEntities()).hasSize(2);
        assertThat(mergedView.getEdges()).hasSize(2);
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

        assertThat(groups).isEqualTo(allGroups);
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
        assertThat(result).isTrue();
    }

    @Test
    public void shouldReturnTrueWhenViewHasPreAggEdgeFilters() {
        // Given
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

        // When
        final boolean result = view.hasPreAggregationFilters();

        // Then
        assertThat(result).isTrue();
    }

    @Test
    public void shouldReturnFalseWhenViewHasNullPreAggEdgeFilters() {
        // Given
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .preAggregationFilter(null)
                        .build())
                .build();

        // When
        final boolean result = view.hasPreAggregationFilters();

        // Then
        assertThat(result).isFalse();
    }

    @Test
    public void shouldReturnFalseWhenViewHasEmptyPreAggEdgeFilters() {
        // Given
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .build())
                        .build())
                .build();

        // When
        final boolean result = view.hasPreAggregationFilters();

        // Then
        assertThat(result).isFalse();
    }

    @Test
    public void shouldReturnTrueWhenViewHasPostAggEntityFilters() {
        // Given
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

        // When
        final boolean result = view.hasPostAggregationFilters();

        // Then
        assertThat(result).isTrue();
    }

    @Test
    public void shouldReturnTrueWhenViewHasPostAggEdgeFilters() {
        // Given
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

        // When
        final boolean result = view.hasPostAggregationFilters();

        // Then
        assertThat(result).isTrue();
    }

    @Test
    public void shouldReturnFalseWhenViewHasNullPostAggEdgeFilters() {
        // Given
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .postAggregationFilter(null)
                        .build())
                .build();

        // When
        final boolean result = view.hasPostAggregationFilters();

        // Then
        assertThat(result).isFalse();
    }

    @Test
    public void shouldReturnFalseWhenViewHasEmptyPostAggEdgeFilters() {
        // Given
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .postAggregationFilter(new ElementFilter.Builder()
                                .build())
                        .build())
                .build();

        // When
        final boolean result = view.hasPostAggregationFilters();

        // Then
        assertThat(result).isFalse();
    }

    @Test
    public void shouldReturnTrueWhenViewHasPostTransformEntityFilters() {
        // Given
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

        // When
        final boolean result = view.hasPostTransformFilters();

        // Then
        assertThat(result).isTrue();
    }

    @Test
    public void shouldReturnTrueWhenViewHasPostTransformEdgeFilters() {
        // Given
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

        // When
        final boolean result = view.hasPostTransformFilters();

        // Then
        assertThat(result).isTrue();
    }

    @Test
    public void shouldReturnFalseWhenViewHasNullPostTransformEdgeFilters() {
        // Given
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .postTransformFilter(null)
                        .build())
                .build();

        // When
        final boolean result = view.hasPostTransformFilters();

        // Then
        assertThat(result).isFalse();
    }

    @Test
    public void shouldReturnFalseWhenViewHasEmptyPostTransformEdgeFilters() {
        // Given
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .postTransformFilter(new ElementFilter.Builder()
                                .build())
                        .build())
                .build();

        // When
        final boolean result = view.hasPostTransformFilters();

        // Then
        assertThat(result).isFalse();
    }

    @Test
    public void shouldAddGlobalPropertiesToEntityGroup() {
        // Given
        final View view = new View.Builder()
                .globalEntities(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .properties(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .build())
                .entity(TestGroups.ENTITY)
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEntity(TestGroups.ENTITY).getProperties())
                .containsExactlyInAnyOrder(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2);
    }

    @Test
    public void shouldSetEmptyEntitiesPropertiesGivenEmptyGlobalProperties() {
        // Given
        final View view = new View.Builder()
                .globalEntities(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .properties()
                        .build())
                .entity(TestGroups.ENTITY)
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEntity(TestGroups.ENTITY).getProperties()).isEmpty();
    }

    @Test
    public void shouldOverrideEmptyGlobalPropertiesAndIncludeEntityGroupProperties() {
        // Given
        final View view = new View.Builder()
                .globalEntities(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .properties()
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder().
                        properties(TestPropertyNames.PROP_1)
                        .build())
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEntity(TestGroups.ENTITY).getProperties())
                .containsExactly(TestPropertyNames.PROP_1);
    }

    @Test
    public void shouldOverrideGlobalPropertiesWhenSpecificEntityGroupPropertiesSet() {
        // Given
        final View view = new View.Builder()
                .globalEntities(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .properties(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .properties(TestPropertyNames.PROP_3)
                        .build())
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEntity(TestGroups.ENTITY).getProperties())
                .containsExactly(TestPropertyNames.PROP_3);
    }

    @Test
    public void shouldAddGlobalExcludePropertiesToEntityGroup() {
        // Given
        final View view = new View.Builder()
                .globalEntities(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .excludeProperties(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder().build())
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEntity(TestGroups.ENTITY).getExcludeProperties())
                .containsExactlyInAnyOrder(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2);
    }

    @Test
    public void shouldOverrideGlobalExcludePropertiesWhenSpecificEntityGroupExcludePropertiesSet() {
        // Given
        final View view = new View.Builder()
                .globalEntities(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .excludeProperties(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .excludeProperties(TestPropertyNames.PROP_3)
                        .build())
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEntity(TestGroups.ENTITY).getExcludeProperties())
                .containsExactly(TestPropertyNames.PROP_3);
    }

    @Test
    public void shouldAddGlobalTransformToEntityGroup() {
        // Given
        final ElementTransformer elementTransformer = new ElementTransformer.Builder()
                .select(TestPropertyNames.PROP_3)
                .execute(new Identity())
                .project(TestPropertyNames.PROP_1)
                .build();

        final View view = new View.Builder()
                .globalEntities(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .transformer(elementTransformer)
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .excludeProperties(TestPropertyNames.PROP_3)
                        .build())
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEntity(TestGroups.ENTITY).getTransformer().getComponents().get(0).getFunction())
                .isExactlyInstanceOf(elementTransformer.getComponents().get(0).getFunction().getClass());
        assertThat(view.getEntity(TestGroups.ENTITY).getExcludeProperties())
                .containsExactly(TestPropertyNames.PROP_3);
    }

    @Test
    public void shouldThrowExceptionWhenGlobalExcludePropertiesAndEntityPropertiesSet() {
        // Given
        final View view = new View.Builder()
                .globalEntities(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .excludeProperties(TestPropertyNames.PROP_2)
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .properties(TestPropertyNames.PROP_2, TestPropertyNames.PROP_3)
                        .build())
                .build();

        // When
        try {
            view.expandGlobalDefinitions();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("You cannot set both properties and excludeProperties");
        }
    }

    @Test
    public void shouldAddGlobalPropertiesToEdgeGroup() {
        // Given
        final View view = new View.Builder()
                .globalEdges(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.EDGE)
                        .properties(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .build())
                .edge(TestGroups.EDGE)
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEdge(TestGroups.EDGE).getProperties())
                .containsExactlyInAnyOrder(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2);
    }

    @Test
    public void shouldSetEmptyEdgePropertiesGivenEmptyGlobalProperties() {
        // Given
        final View view = new View.Builder()
                .globalEdges(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.EDGE)
                        .properties()
                        .build())
                .edge(TestGroups.EDGE)
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEdge(TestGroups.EDGE).getProperties()).isEmpty();
    }

    @Test
    public void shouldOverrideEmptyGlobalPropertiesAndIncludeEdgeGroupProperties() {
        // Given
        final View view = new View.Builder()
                .globalEdges(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.EDGE)
                        .properties()
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder().
                        properties(TestPropertyNames.PROP_1)
                        .build())
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEdge(TestGroups.EDGE).getProperties())
                .containsExactly(TestPropertyNames.PROP_1);
    }

    @Test
    public void shouldOverrideGlobalPropertiesWhenSpecificEdgeGroupPropertiesSet() {
        // Given
        final View view = new View.Builder()
                .globalEdges(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.EDGE)
                        .properties(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .properties(TestPropertyNames.PROP_3)
                        .build())
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEdge(TestGroups.EDGE).getProperties())
                .containsExactly(TestPropertyNames.PROP_3);
    }

    @Test
    public void shouldAddGlobalExcludePropertiesToEdgeGroup() {
        // Given
        final View view = new View.Builder()
                .globalEdges(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.EDGE)
                        .excludeProperties(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder().build())
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEdge(TestGroups.EDGE).getExcludeProperties())
                .containsExactlyInAnyOrder(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2);
    }

    @Test
    public void shouldOverrideGlobalExcludePropertiesWhenSpecificEdgeGroupExcludePropertiesSet() {
        // Given
        final View view = new View.Builder()
                .globalEdges(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.EDGE)
                        .excludeProperties(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .excludeProperties(TestPropertyNames.PROP_3)
                        .build())
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEdge(TestGroups.EDGE).getExcludeProperties())
                .containsExactly(TestPropertyNames.PROP_3);
    }

    @Test
    public void shouldAddGlobalTransformToEdgeGroup() {
        // Given
        final ElementTransformer elementTransformer = new ElementTransformer.Builder()
                .select(TestPropertyNames.PROP_3)
                .execute(new Identity())
                .project(TestPropertyNames.PROP_1)
                .build();

        final View view = new View.Builder()
                .globalEdges(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.EDGE)
                        .transformer(elementTransformer)
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .excludeProperties(TestPropertyNames.PROP_3)
                        .build())
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEdge(TestGroups.EDGE).getTransformer().getComponents().get(0).getFunction())
                .isExactlyInstanceOf(elementTransformer.getComponents().get(0).getFunction().getClass());
        assertThat(view.getEdge(TestGroups.EDGE).getExcludeProperties())
                .containsExactly(TestPropertyNames.PROP_3);
    }

    @Test
    public void shouldThrowExceptionWhenGlobalExcludePropertiesAndEdgePropertiesSet() {
        // Given
        final View view = new View.Builder()
                .globalEdges(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.EDGE)
                        .excludeProperties(TestPropertyNames.PROP_2)
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .properties(TestPropertyNames.PROP_2, TestPropertyNames.PROP_3)
                        .build())
                .build();

        // When
        try {
            view.expandGlobalDefinitions();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("You cannot set both properties and excludeProperties");
        }
    }

    @Test
    public void shouldAddGlobalElementPropertiesToGroup() {
        // Given
        final View view = new View.Builder()
                .globalElements(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .properties(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .build())
                .entity(TestGroups.ENTITY)
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEntity(TestGroups.ENTITY).getProperties())
                .containsExactlyInAnyOrder(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2);
    }

    @Test
    public void shouldSetEmptyPropertiesGivenEmptyGlobalElementProperties() {
        // Given
        final View view = new View.Builder()
                .globalElements(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .properties()
                        .build())
                .entity(TestGroups.ENTITY)
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEntity(TestGroups.ENTITY).getProperties()).isEmpty();
    }

    @Test
    public void shouldOverrideEmptyGlobalElementPropertiesAndIncludeEntityGroupProperties() {
        // Given
        final View view = new View.Builder()
                .globalElements(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .properties()
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder().
                        properties(TestPropertyNames.PROP_1)
                        .build())
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEntity(TestGroups.ENTITY).getProperties())
                .containsExactly(TestPropertyNames.PROP_1);
    }

    @Test
    public void shouldOverrideGlobalElementPropertiesWhenSpecificEntityGroupPropertiesSet() {
        // Given
        final View view = new View.Builder()
                .globalElements(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .properties(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .properties(TestPropertyNames.PROP_3)
                        .build())
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEntity(TestGroups.ENTITY).getProperties())
                .containsExactly(TestPropertyNames.PROP_3);
    }

    @Test
    public void shouldAddGlobalExcludeElementPropertiesToEntityGroup() {
        // Given
        final View view = new View.Builder()
                .globalElements(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .excludeProperties(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder().build())
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEntity(TestGroups.ENTITY).getExcludeProperties())
                .containsExactlyInAnyOrder(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2);
    }

    @Test
    public void shouldOverrideGlobalExcludeElementPropertiesWhenSpecificEntityGroupExcludePropertiesSet() {
        // Given
        final View view = new View.Builder()
                .globalElements(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .excludeProperties(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .excludeProperties(TestPropertyNames.PROP_3)
                        .build())
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEntity(TestGroups.ENTITY).getExcludeProperties())
                .containsExactly(TestPropertyNames.PROP_3);
    }

    @Test
    public void shouldAddGlobalElementTransformToEntityGroupFromBuilder() {
        // Given
        final ElementTransformer elementTransformer = new ElementTransformer.Builder()
                .select(TestPropertyNames.PROP_3)
                .execute(new Identity())
                .project(TestPropertyNames.PROP_1)
                .build();

        // When
        final View view = new View.Builder()
                .globalElements(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .transformer(elementTransformer)
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .excludeProperties(TestPropertyNames.PROP_3)
                        .build())
                .expandGlobalDefinitions()
                .build();

        // Then
        assertThat(view.getEntity(TestGroups.ENTITY).getTransformer().getComponents().get(0).getFunction())
                .isExactlyInstanceOf(elementTransformer.getComponents().get(0).getFunction().getClass());
        assertThat(view.getEntity(TestGroups.ENTITY).getExcludeProperties())
                .containsExactly(TestPropertyNames.PROP_3);
    }

    @Test
    public void shouldAddGlobalElementTransformToEntityGroup() {
        // Given
        final ElementTransformer elementTransformer = new ElementTransformer.Builder()
                .select(TestPropertyNames.PROP_3)
                .execute(new Identity())
                .project(TestPropertyNames.PROP_1)
                .build();

        final View view = new View.Builder()
                .globalElements(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .transformer(elementTransformer)
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .excludeProperties(TestPropertyNames.PROP_3)
                        .build())
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.getEntity(TestGroups.ENTITY).getTransformer().getComponents().get(0).getFunction())
                .isExactlyInstanceOf(elementTransformer.getComponents().get(0).getFunction().getClass());
        assertThat(view.getEntity(TestGroups.ENTITY).getExcludeProperties())
                .containsExactly(TestPropertyNames.PROP_3);
    }

    @Test
    public void shouldThrowExceptionWhenGlobalExcludeElementPropertiesAndEntityPropertiesSet() {
        // Given
        final View view = new View.Builder()
                .globalElements(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .excludeProperties(TestPropertyNames.PROP_2)
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .properties(TestPropertyNames.PROP_2, TestPropertyNames.PROP_3)
                        .build())
                .build();

        // When
        try {
            view.expandGlobalDefinitions();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("You cannot set both properties and excludeProperties");
        }
    }

    @Test
    public void shouldAddGlobalPreAggFiltersToGroup() {
        // Given
        final ElementFilter filter = new ElementFilter.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(new Exists())
                .build();

        final View view = new View.Builder()
                .globalEntities(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .preAggregationFilter(filter)
                        .build())
                .entity(TestGroups.ENTITY)
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.hasPreAggregationFilters()).isTrue();
        assertThat(view.getEntity(TestGroups.ENTITY).getPreAggregationFilter()
                        .getComponents().get(0).getPredicate())
                .isExactlyInstanceOf(Exists.class);
    }

    @Test
    public void shouldConcatGlobalPreAggFiltersWhenSpecificGroupPreAggFiltersSet() {
        // Given
        final ElementFilter globalFilter = new ElementFilter.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(new Exists())
                .build();

        final ElementFilter groupFilter = new ElementFilter.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(new ExampleFilterFunction())
                .build();

        final View view = new View.Builder()
                .globalEntities(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .preAggregationFilter(globalFilter)
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .preAggregationFilter(groupFilter)
                        .build())
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.hasPreAggregationFilters()).isTrue();
        assertThat(view.getEntity(TestGroups.ENTITY).getPreAggregationFilter()
                        .getComponents().get(0).getPredicate())
                .isExactlyInstanceOf(Exists.class);
        assertThat(view.getEntity(TestGroups.ENTITY).getPreAggregationFilter()
                        .getComponents().get(1).getPredicate())
                .isExactlyInstanceOf(ExampleFilterFunction.class);
    }

    @Test
    public void shouldAddGlobalPostAggFiltersToGroup() {
        // Given
        final ElementFilter filter = new ElementFilter.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(new Exists())
                .build();

        final View view = new View.Builder()
                .globalEntities(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .postAggregationFilter(filter)
                        .build())
                .entity(TestGroups.ENTITY)
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.hasPostAggregationFilters()).isTrue();
        assertThat(view.getEntity(TestGroups.ENTITY).getPostAggregationFilter()
                        .getComponents().get(0).getPredicate())
                .isExactlyInstanceOf(Exists.class);
    }

    @Test
    public void shouldConcatGlobalPostAggFiltersWhenSpecificGroupPostAggFiltersSet() {
        // Given
        final ElementFilter globalFilter = new ElementFilter.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(new Exists())
                .build();

        final ElementFilter groupFilter = new ElementFilter.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(new ExampleFilterFunction())
                .build();

        final View view = new View.Builder()
                .globalEntities(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .postAggregationFilter(globalFilter)
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .postAggregationFilter(groupFilter)
                        .build())
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.hasPostAggregationFilters()).isTrue();
        assertThat(view.getEntity(TestGroups.ENTITY).getPostAggregationFilter()
                        .getComponents().get(0).getPredicate())
                .isExactlyInstanceOf(Exists.class);
        assertThat(view.getEntity(TestGroups.ENTITY).getPostAggregationFilter()
                        .getComponents().get(1).getPredicate())
                .isExactlyInstanceOf(ExampleFilterFunction.class);
    }

    @Test
    public void shouldAddGlobalPostTransformFiltersToGroup() {
        // Given
        final ElementFilter filter = new ElementFilter.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(new Exists())
                .build();

        final View view = new View.Builder()
                .globalEntities(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .postTransformFilter(filter)
                        .build())
                .entity(TestGroups.ENTITY)
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.hasPostTransformFilters()).isTrue();
        assertThat(view.getEntity(TestGroups.ENTITY).getPostTransformFilter()
                        .getComponents().get(0).getPredicate())
                .isExactlyInstanceOf(Exists.class);
    }

    @Test
    public void shouldConcatGlobalPostTransformFiltersWhenSpecificGroupPostTransformFiltersSet() {
        // Given
        final ElementFilter globalFilter = new ElementFilter.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(new Exists())
                .build();

        final ElementFilter groupFilter = new ElementFilter.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(new ExampleFilterFunction())
                .build();

        final View view = new View.Builder()
                .globalEntities(new GlobalViewElementDefinition.Builder()
                        .groups(TestGroups.ENTITY)
                        .postTransformFilter(globalFilter)
                        .build())
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .postTransformFilter(groupFilter)
                        .build())
                .build();

        // When
        view.expandGlobalDefinitions();

        // Then
        assertThat(view.hasPostTransformFilters()).isTrue();
        assertThat(view.getEntity(TestGroups.ENTITY).getPostTransformFilter()
                        .getComponents().get(0).getPredicate())
                .isExactlyInstanceOf(Exists.class);
        assertThat(view.getEntity(TestGroups.ENTITY).getPostTransformFilter()
                        .getComponents().get(1).getPredicate())
                .isExactlyInstanceOf(ExampleFilterFunction.class);
    }

    @Test
    public void shouldFilterEntitiesInBuilder() {
        // When
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY)
                .entity(TestGroups.ENTITY_2)
                .removeEntities(e -> e.getKey().equals(TestGroups.ENTITY))
                .build();

        // Then
        assertThat(view.getEntityGroups()).containsExactly(TestGroups.ENTITY_2);
    }

    @Test
    public void shouldFilterEdgesInBuilder() {
        // When
        final View view = new View.Builder()
                .edge(TestGroups.EDGE)
                .edge(TestGroups.EDGE_2)
                .removeEdges(e -> e.getKey().equals(TestGroups.EDGE))
                .build();

        // Then
        assertThat(view.getEdgeGroups()).containsExactly(TestGroups.EDGE_2);
    }

    @Test
    public void shouldCopyAllEntitiesFlagsWhenCloned() {
        // Given
        final ViewElementDefinition edgeDef1 = new ViewElementDefinition();

        // When
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, edgeDef1)
                .allEntities(true)
                .build();

        // Then
        final View clone = view.clone();

        // Check that the objects are equal
        assertThat(clone.isAllEntities()).isEqualTo(view.isAllEntities());
    }

    @Test
    public void shouldCopyAllEdgesFlagsWhenCloned() {
        // Given
        final ViewElementDefinition edgeDef1 = new ViewElementDefinition();

        // When
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, edgeDef1)
                .allEdges(true)
                .build();

        // Then
        final View clone = view.clone();

        // Check that the objects are equal
        assertThat(clone.isAllEntities()).isEqualTo(view.isAllEntities());
    }

    @Test
    public void shouldCopyAllEdgesEntitiesFlagsWhenCloned() {
        // Given
        final ViewElementDefinition edgeDef1 = new ViewElementDefinition();

        // When
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, edgeDef1)
                .allEntities(true)
                .allEdges(true)
                .build();

        // Then
        final View clone = view.clone();

        // Check that the objects are equal
        assertThat(clone.isAllEntities()).isEqualTo(view.isAllEntities());
    }

    @Test
    public void shouldCopyAllEdgesEntitiesNotFlagsWhenCloned() {
        // Given
        final ViewElementDefinition edgeDef1 = new ViewElementDefinition();

        // When
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, edgeDef1)
                .build();

        // Then
        final View clone = view.clone();

        // Check that the objects are equal
        assertThat(clone.isAllEntities()).isEqualTo(view.isAllEntities());
    }

    @Test
    public void shouldCopyAllConfigMapsEdgesEntitiesFlagsWhenCloned() {
        // Given
        final ViewElementDefinition edgeDef1 = new ViewElementDefinition();

        HashMap<String, String> config = new HashMap<>();
        config.put("config.test", "config");

        // When
        final View view = new View.Builder()
                .config(config)
                .edge(TestGroups.EDGE, edgeDef1)
                .allEntities(true)
                .allEdges(true)
                .build();

        // Then
        final View clone = view.clone();

        // Check that the objects are equal
        assertThat(clone.isAllEntities()).isEqualTo(view.isAllEntities());
    }

    @Test
    public void shouldCopyAllConfigEdgesEntitiesFlagsWhenCloned() {
        // Given
        final ViewElementDefinition edgeDef1 = new ViewElementDefinition();

        // When
        final View view = new View.Builder()
                .config("config.test", "config")
                .edge(TestGroups.EDGE, edgeDef1)
                .allEntities(true)
                .allEdges(true)
                .build();

        // Then
        final View clone = view.clone();

        // Check that the objects are equal
        assertThat(clone.isAllEntities()).isEqualTo(view.isAllEntities());
    }

    @Test
    public void shouldCloneUsingBuilderWithViewInJsonFormat() {
        // Given
        final ViewElementDefinition edgeDef1 = new ViewElementDefinition();

        // When
        final View view = new View.Builder()
                .config("config.test", "config")
                .edge(TestGroups.EDGE, edgeDef1)
                .allEntities(true)
                .allEdges(true)
                .build();

        // Then
        byte[] json = view.toJson(true);

        // Check that the objects are equal
        View clone = new View.Builder().json(json).build();
        assertThat(clone).isEqualTo(view);
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
                .config("key1", "value1")
                .build();
    }
}
