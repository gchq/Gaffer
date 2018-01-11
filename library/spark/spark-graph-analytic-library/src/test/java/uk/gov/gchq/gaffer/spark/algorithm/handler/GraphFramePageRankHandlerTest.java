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

package uk.gov.gchq.gaffer.spark.algorithm.handler;

import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;
import org.junit.Test;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.algorithm.GraphFramePageRank;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GraphFramePageRankHandlerTest {

    private final static Integer MAX_ITERATIONS = 10;
    private final static Double TOLERANCE = 1E-3;
    private final static Double RESET_PROBABILITY = 0.15;

    @Test
    public void shouldGetCorrectPageRankForGraphFrameUsingMaxIterations() throws OperationException {

        // Given
        final GraphFrame input = mock(GraphFrame.class);
        final PageRank pageRank = mock(PageRank.class);

        final GraphFramePageRankHandler handler = new GraphFramePageRankHandler();

        final GraphFramePageRank operation = new GraphFramePageRank.Builder()
                .input(input)
                .resetProbability(RESET_PROBABILITY)
                .maxIterations(MAX_ITERATIONS)
                .build();

        when(input.pageRank()).thenReturn(pageRank);

        when(pageRank.resetProbability(RESET_PROBABILITY)).thenReturn(pageRank);
        when(pageRank.maxIter(MAX_ITERATIONS)).thenReturn(pageRank);
        when(pageRank.run()).thenReturn(input);

        // When
        final GraphFrame result = handler.doOperation(operation, null, null);

        // Then
        verify(input, times(1)).pageRank();
        verify(pageRank, times(1)).run();
        verify(pageRank, times(1)).maxIter(MAX_ITERATIONS);
    }

    @Test
    public void shouldGetCorrectPageRankForGraphFrameUsingTolerance() throws OperationException {
        // Given
        final GraphFrame input = mock(GraphFrame.class);
        final PageRank pageRank = mock(PageRank.class);

        final GraphFramePageRankHandler handler = new GraphFramePageRankHandler();

        final GraphFramePageRank operation = new GraphFramePageRank.Builder()
                .input(input)
                .resetProbability(RESET_PROBABILITY)
                .tolerance(TOLERANCE)
                .build();

        when(input.pageRank()).thenReturn(pageRank);

        when(pageRank.resetProbability(RESET_PROBABILITY)).thenReturn(pageRank);
        when(pageRank.tol(TOLERANCE)).thenReturn(pageRank);
        when(pageRank.run()).thenReturn(input);

        // When
        final GraphFrame result = handler.doOperation(operation, null, null);

        // Then
        verify(input, times(1)).pageRank();
        verify(pageRank, times(1)).run();
        verify(pageRank, times(1)).tol(TOLERANCE);
    }
}
