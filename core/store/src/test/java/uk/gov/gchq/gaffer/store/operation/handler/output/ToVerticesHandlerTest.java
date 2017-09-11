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

package uk.gov.gchq.gaffer.store.operation.handler.output;

import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices.EdgeVertices;
import uk.gov.gchq.gaffer.store.Context;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ToVerticesHandlerTest {

    @Test
    public void shouldConvertElementSeedsToVertices() throws OperationException {
        // Given
        final Object vertex1 = "vertex1";
        final Object vertex2 = "vertex2";

        final List elementIds = Arrays.asList(new EntitySeed(vertex1), new EntitySeed(vertex2));

        final ToVerticesHandler handler = new ToVerticesHandler();
        final ToVertices operation = mock(ToVertices.class);

        given(operation.getInput()).willReturn(elementIds);
        given(operation.getEdgeVertices()).willReturn(EdgeVertices.NONE);

        //When
        final Iterable<Object> results = handler.doOperation(operation, new Context(), null);

        //Then
        assertThat(results, containsInAnyOrder(vertex1, vertex2));
    }

    @Test
    public void shouldBeAbleToIterableOverTheResultsMultipleTimes() throws OperationException {
        // Given
        final Object vertex1 = "vertex1";
        final Object vertex2 = "vertex2";

        final List elementIds = Arrays.asList(new EntitySeed(vertex1), new EntitySeed(vertex2));

        final ToVerticesHandler handler = new ToVerticesHandler();
        final ToVertices operation = mock(ToVertices.class);

        given(operation.getInput()).willReturn(elementIds);
        given(operation.getEdgeVertices()).willReturn(EdgeVertices.NONE);

        //When
        final Iterable<Object> results = handler.doOperation(operation, new Context(), null);

        //Then
        final Set<Object> set1 = Sets.newHashSet(results);
        final Set<Object> set2 = Sets.newHashSet(results);
        assertEquals(Sets.newHashSet(vertex1, vertex2), set1);
        assertEquals(set1, set2);
    }

    @Test
    public void shouldConvertEdgeSeedsToVertices_matchedVertexEqual() throws OperationException {
        // Given
        final Object vertex1 = "vertex1";
        final Object vertex2 = "vertex2";
        final Object vertex3 = "vertex3";
        final Object vertex4 = "vertex4";
        final Object vertex5 = "vertex5";
        final Object vertex6 = "vertex6";
        final Object vertex7 = "vertex7";
        final Object vertex8 = "vertex8";

        final List elementIds = Arrays.asList(
                new EdgeSeed(vertex1, vertex2, false, EdgeId.MatchedVertex.SOURCE),
                new EdgeSeed(vertex3, vertex4, false, EdgeId.MatchedVertex.DESTINATION),
                new EdgeSeed(vertex5, vertex6, false, EdgeId.MatchedVertex.DESTINATION),
                new EdgeSeed(vertex7, vertex8, false, null)
        );

        final ToVerticesHandler handler = new ToVerticesHandler();
        final ToVertices operation = mock(ToVertices.class);

        given(operation.getInput()).willReturn(elementIds);
        given(operation.getUseMatchedVertex()).willReturn(ToVertices.UseMatchedVertex.EQUAL);
        given(operation.getEdgeVertices()).willReturn(EdgeVertices.DESTINATION);

        //When
        final Iterable<Object> results = handler.doOperation(operation, new Context(), null);

        //Then
        assertThat(Sets.newHashSet(results), containsInAnyOrder(vertex1, vertex4, vertex6, vertex8));
    }

    @Test
    public void shouldConvertEdgeSeedsToVertices_matchedVertexOpposite() throws OperationException {
        // Given
        final Object vertex1 = "vertex1";
        final Object vertex2 = "vertex2";
        final Object vertex3 = "vertex3";
        final Object vertex4 = "vertex4";
        final Object vertex5 = "vertex5";
        final Object vertex6 = "vertex6";
        final Object vertex7 = "vertex7";
        final Object vertex8 = "vertex8";

        final List elementIds = Arrays.asList(
                new EdgeSeed(vertex1, vertex2, false, EdgeId.MatchedVertex.SOURCE),
                new EdgeSeed(vertex3, vertex4, false, EdgeId.MatchedVertex.DESTINATION),
                new EdgeSeed(vertex5, vertex6, false, EdgeId.MatchedVertex.DESTINATION),
                new EdgeSeed(vertex7, vertex8, false, null)
        );

        final ToVerticesHandler handler = new ToVerticesHandler();
        final ToVertices operation = mock(ToVertices.class);

        given(operation.getInput()).willReturn(elementIds);
        given(operation.getUseMatchedVertex()).willReturn(ToVertices.UseMatchedVertex.OPPOSITE);
        given(operation.getEdgeVertices()).willReturn(EdgeVertices.SOURCE);

        //When
        final Iterable<Object> results = handler.doOperation(operation, new Context(), null);

        //Then
        assertThat(Sets.newHashSet(results), containsInAnyOrder(vertex2, vertex3, vertex5, vertex7));
    }

    @Test
    public void shouldConvertEdgeSeedsToVertices_sourceAndDestination() throws OperationException {
        // Given
        final Object vertex1 = "vertex1";
        final Object vertex2 = "vertex2";
        final Object vertex3 = "vertex3";

        final List elementIds = Arrays.asList(new EdgeSeed(vertex1, vertex2, false), new EdgeSeed(vertex1, vertex3, false));

        final ToVerticesHandler handler = new ToVerticesHandler();
        final ToVertices operation = mock(ToVertices.class);

        given(operation.getInput()).willReturn(elementIds);
        given(operation.getEdgeVertices()).willReturn(EdgeVertices.BOTH);

        //When
        final Iterable<Object> results = handler.doOperation(operation, new Context(), null);

        //Then
        assertThat(Sets.newHashSet(results), containsInAnyOrder(vertex1, vertex2, vertex3));
    }

    @Test
    public void shouldConvertEdgeSeedsToVertices_sourceOnly() throws OperationException {
        // Given
        final Object vertex1 = "vertex1";
        final Object vertex2 = "vertex2";

        final List elementIds = Collections.singletonList(new EdgeSeed(vertex1, vertex2, false));
        final ToVerticesHandler handler = new ToVerticesHandler();
        final ToVertices operation = mock(ToVertices.class);

        given(operation.getInput()).willReturn(elementIds);
        given(operation.getEdgeVertices()).willReturn(EdgeVertices.SOURCE);

        //When
        final Iterable<Object> results = handler.doOperation(operation, new Context(), null);

        //Then
        assertThat(Sets.newHashSet(results), containsInAnyOrder(vertex1));
    }

    @Test
    public void shouldConvertEdgeSeedsToVertices_destinationOnly() throws OperationException {
        // Given
        final Object vertex1 = "vertex1";
        final Object vertex2 = "vertex2";

        final List elementIds = Collections.singletonList(new EdgeSeed(vertex1, vertex2, false));

        final ToVerticesHandler handler = new ToVerticesHandler();
        final ToVertices operation = mock(ToVertices.class);

        given(operation.getInput()).willReturn(elementIds);
        given(operation.getEdgeVertices()).willReturn(EdgeVertices.DESTINATION);

        //When
        final Iterable<Object> results = handler.doOperation(operation, new Context(), null);

        //Then
        assertThat(Sets.newHashSet(results), containsInAnyOrder(vertex2));
    }

    @Test
    public void shouldHandleNullInput() throws OperationException {
        // Given
        final ToVerticesHandler handler = new ToVerticesHandler();
        final ToVertices operation = mock(ToVertices.class);

        given(operation.getInput()).willReturn(null);
        given(operation.getEdgeVertices()).willReturn(EdgeVertices.NONE);

        //When
        final Iterable<Object> results = handler.doOperation(operation, new Context(), null);

        //Then
        assertThat(results, is(nullValue()));
    }
}
