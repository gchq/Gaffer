/*
 * Copyright 2017 Crown Copyright
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

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.function.ExampleFilterFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class NamedViewTest {

    private static final String TEST_VIEW_NAME = "testViewName";

    @Test
    public void shouldCreateEmptyNamedViewWithBasicConstructor() {
        //When
        NamedView namedView = new NamedView();

        //Then
        assertTrue(namedView.getName().isEmpty());
        assertTrue(namedView.getParameters().isEmpty());
        assertTrue(namedView.getEdges().isEmpty());
        assertTrue(namedView.getEntities().isEmpty());
    }

    @Test
    public void shouldCreateNewNamedViewWithEdgesAndEntities() {
        //Given
        List<String> entityGroups = new ArrayList<>();
        List<String> edgeGroups = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            entityGroups.add(TestGroups.ENTITY + i);
            edgeGroups.add(TestGroups.EDGE + i);
        }

        //When
        NamedView namedView = new NamedView.Builder()
                .entities(entityGroups)
                .edges(edgeGroups)
                .build();

        //Then
        assertTrue(namedView.getEntityGroups().containsAll(entityGroups));
        assertEquals(entityGroups.size(), namedView.getEntityGroups().size());
        assertTrue(namedView.getEdgeGroups().containsAll(edgeGroups));
        assertEquals(edgeGroups.size(), namedView.getEdgeGroups().size());
    }

    @Test
    public void shouldBuildFullNamedView() {
        // Given
        final ViewElementDefinition edgeDef1 = new ViewElementDefinition();
        final ViewElementDefinition entityDef1 = new ViewElementDefinition();
        final Map<String, Object> testParameters = new HashMap<>();
        testParameters.put("testParamKey", "test");

        // When
        NamedView namedView = new NamedView.Builder()
                .edge(TestGroups.EDGE, edgeDef1)
                .entity(TestGroups.ENTITY, entityDef1)
                .name(TEST_VIEW_NAME)
                .parameters(testParameters)
                .build();

        // Then
        assertEquals(1, namedView.getEdges().size());
        assertSame(edgeDef1, namedView.getEdge(TestGroups.EDGE));


        assertEquals(1, namedView.getEntities().size());
        assertSame(entityDef1, namedView.getEntity(TestGroups.ENTITY));

        assertEquals(TEST_VIEW_NAME, namedView.getName());
        assertEquals(testParameters, namedView.getParameters());
    }

    @Test
    public void shouldSerialiseToJson() {
        // Given
        final NamedView namedView = new NamedView.Builder()
                .name(TEST_VIEW_NAME)
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .build();

        // When
        byte[] json = namedView.toJson(true);

        // Then
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView\"," +
                "  \"edges\" : { }," +
                "  \"entities\" : {\n" +
                "    \"BasicEntity\" : {\n" +
                "       \"preAggregationFilterFunctions\" : [ {\n" +
                "          \"predicate\" : {\n" +
                "             \"class\" : \"uk.gov.gchq.gaffer.function.ExampleFilterFunction\"" +
                "           }," +
                "           \"selection\" : [ \"property1\" ]" +
                "          } ]" +
                "        }" +
                "      }," +
                "      \"name\": \"testViewName\"," +
                "      \"parameters\" : { }" +
                "    }"), new String(json));
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() {
        // Given
        final ViewElementDefinition edgeDef1 = new ViewElementDefinition();
        final ViewElementDefinition entityDef1 = new ViewElementDefinition();
        final Map<String, Object> testParameters = new HashMap<>();
        testParameters.put("testParamKey", "test");
        NamedView namedView = new NamedView.Builder()
                .edge(TestGroups.EDGE, edgeDef1)
                .entity(TestGroups.ENTITY, entityDef1)
                .name(TEST_VIEW_NAME)
                .parameters(testParameters)
                .build();

        byte[] json = namedView.toJson(true);
        final NamedView deserialisedView = new NamedView.Builder().json(json).build();

        assertEquals(TEST_VIEW_NAME, deserialisedView.getName());
        assertEquals(testParameters, namedView.getParameters());
        assertEquals(1, namedView.getEdges().size());
        assertSame(edgeDef1, namedView.getEdge(TestGroups.EDGE));

        assertEquals(1, namedView.getEntities().size());
        assertSame(entityDef1, namedView.getEntity(TestGroups.ENTITY));
    }

    @Test
    public void shouldMergeNamedViews() {

    }
}
