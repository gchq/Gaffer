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

package uk.gov.gchq.gaffer.spark.operation.graphframe;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;
import org.graphframes.GraphFrame;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.spark.serialisation.TypeReferenceSparkImpl;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.Map;

/**
 * A {@code PageRank} is an operation which uses the Databricks GraphFrames
 * library to execute the PageRank algorithm on an input graph.
 * <p>
 * Users specifying a PageRank operation MUST provide one of the following: <ul>
 * <li>tolerance - set the desired precision of the PageRank values. With this
 * setting PageRank will run continuously until convergence</li>
 * <li>maxIterations - set the maximum number of iterations before
 * returning</li> </ul>
 * <p>
 * Setting both of these values will result in an error.
 */
public class PageRank implements
        InputOutput<GraphFrame, GraphFrame> {

    public GraphFrame input;
    public Map<String, String> options;
    private Double tolerance;
    private Integer maxIterations;

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
            result.addError("One of maxIter or tol must be specified.");
        }

        return result;
    }

    @Override
    public TypeReference<GraphFrame> getOutputTypeReference() {
        return new TypeReferenceSparkImpl.GraphFrame();
    }

    @Override
    public Operation shallowClone() throws CloneFailedException {
        return new PageRank.Builder()
                .input(input)
                .maxIterations(maxIterations)
                .tolerance(tolerance)
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

    public static class Builder extends Operation.BaseBuilder<PageRank, Builder>
            implements InputOutput.Builder<PageRank, GraphFrame, GraphFrame, Builder> {

        public Builder() {
            super(new PageRank());
        }

        public Builder maxIterations(final Integer maxIterations) {
            _getOp().setMaxIterations(maxIterations);
            return _self();
        }

        public Builder tolerance(final Double tolerance) {
            _getOp().setTolerance(tolerance);
            return _self();
        }
    }
}
