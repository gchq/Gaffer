/*
 * Copyright 2016 Crown Copyright
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

package gaffer.data.elementdefinition.view;

import gaffer.commonutil.TestGroups;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

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
            entityGroups.add("entityGroup" + i);
            edgeGroups.add("edgeGroup" + i);
        }

        //When
        View view = new View(entityGroups, edgeGroups);

        //Then
        assertTrue(view.validate());

    }

    @Test
    public void shouldFailValidationBySharingGroupsAcrossEntitiesAndEdges() {
        //Given
        List<String> entityGroups = new ArrayList<>();
        List<String> edgeGroups = new ArrayList<>();

        entityGroups.add("FailureElement");
        edgeGroups.add("FailureElement");

        //When
        View view = new View(entityGroups, edgeGroups);

        //Then
        assertFalse(view.validate());
    }

    @Test
    public void shouldBuildView() {
        // Given
        final ViewEdgeDefinition edgeDef1 = mock(ViewEdgeDefinition.class);
        final ViewEdgeDefinition edgeDef2 = mock(ViewEdgeDefinition.class);
        final ViewEntityDefinition entityDef1 = mock(ViewEntityDefinition.class);
        final ViewEntityDefinition entityDef2 = mock(ViewEntityDefinition.class);

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
}
