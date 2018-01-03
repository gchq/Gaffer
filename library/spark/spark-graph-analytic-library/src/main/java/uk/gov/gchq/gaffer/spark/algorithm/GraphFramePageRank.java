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
import org.apache.commons.lang3.exception.CloneFailedException;
import org.graphframes.GraphFrame;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.spark.serialisation.TypeReferenceSparkImpl;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.Map;

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
public class GraphFramePageRank implements
        PageRank<GraphFrame, GraphFrame> {

    public GraphFrame input;
    public Map<String, String> options;

    /**
     * The desired precision of the PageRank values. Cannot
     * be used in conjunction with the maxIterations field.
     */
    private Double tolerance;

    /**
     * The maximum number of iterations before terminating the operation. Cannot
     * be used in conjunction with the tolerance field.
     */
    private Integer maxIterations;

    /**
     * The probability the algorithm starts from a node chosen uniformly at random
     * among all nodes in the network. (Also known as the damping factor.
     */
    private Double resetProbability = 0.15d;

    @Override
    public GraphFrame getInput() {
        return input;
    }

    @Override
    public void setInput(final GraphFrame input) {
        this.input = input;
    }

    @Override
    public ValidationResult validate() {
        final ValidationResult result = InputOutput.super.validate();

        if (null != maxIterations && null != tolerance) {
            result.addError("Must not specify both maxIterations and tolerance.");
        }

        if (null == maxIterations && null == tolerance) {
            result.addError("One of maxIterations or tolerance must be specified.");
        }

        return result;
    }

    @Override
    public TypeReference<GraphFrame> getOutputTypeReference() {
        return new TypeReferenceSparkImpl.GraphFrame();
    }

    @Override
    public GraphFramePageRank shallowClone() throws CloneFailedException {
        return new GraphFramePageRank.Builder()
                .input(input)
                .maxIterations(maxIterations)
                .tolerance(tolerance)
                .resetProbability(resetProbability)
                .options(options)
                .build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public Integer getMaxIterations() {
        return maxIterations;
    }

    public void setMaxIterations(final Integer maxIterations) {
        this.maxIterations = maxIterations;
    }

    public Double getTolerance() {
        return tolerance;
    }

    public void setTolerance(final Double tolerance) {
        this.tolerance = tolerance;
    }

    public Double getResetProbability() {
        return resetProbability;
    }

    public void setResetProbability(final Double resetProbability) {
        this.resetProbability = resetProbability;
    }

    public static class Builder extends Operation.BaseBuilder<GraphFramePageRank, Builder>
            implements InputOutput.Builder<GraphFramePageRank, GraphFrame, GraphFrame, Builder> {

        public Builder() {
            super(new GraphFramePageRank());
        }

        public Builder maxIterations(final Integer maxIterations) {
            _getOp().setMaxIterations(maxIterations);
            return _self();
        }

        public Builder tolerance(final Double tolerance) {
            _getOp().setTolerance(tolerance);
            return _self();
        }

        public Builder resetProbability(final Double resetProbability) {
            _getOp().setResetProbability(resetProbability);
            return _self();
        }
    }
}
