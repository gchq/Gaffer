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

package uk.gov.gchq.gaffer.spark.algorithm;

import com.fasterxml.jackson.core.type.TypeReference;
import org.graphframes.GraphFrame;

import uk.gov.gchq.gaffer.algorithm.PageRank;
import uk.gov.gchq.gaffer.spark.serialisation.TypeReferenceSparkImpl;

/**
 * A {@code GraphFramePageRank} is an operation which uses the Databricks GraphFrames
 * library to execute the PageRank algorithm on an input graph.
 * <p>
 * Users specifying a GraphFramePageRank operation MUST provide one of the following:
 * <ul>
 * <li>tolerance - set the desired precision of the PageRank values. With this
 * setting PageRank will run continuously until convergence</li>
 * <li>maxIterations - set the maximum number of iterations before
 * returning</li>
 * </ul>
 * <p>
 * Setting both of these values will result in an error.
 */
public class GraphFramePageRank extends PageRank<GraphFrame> {

    @Override
    public TypeReference<GraphFrame> getOutputTypeReference() {
        return new TypeReferenceSparkImpl.GraphFrame();
    }

    @Override
    public GraphFramePageRank shallowClone() {
        return new GraphFramePageRank.Builder()
                .input(input)
                .maxIterations(maxIterations)
                .tolerance(tolerance)
                .resetProbability(resetProbability)
                .options(options)
                .build();
    }

    public static class Builder extends BaseBuilder<GraphFramePageRank, Builder>
            implements PageRank.PageRankBuilder<GraphFramePageRank, GraphFrame, Builder> {

        public Builder() {
            super(new GraphFramePageRank());
        }
    }
}
