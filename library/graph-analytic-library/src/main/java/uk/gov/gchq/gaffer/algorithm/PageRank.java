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
package uk.gov.gchq.gaffer.algorithm;

import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.Map;

/**
 * A {@code PageRank} is an operation which determines the PageRank values for an
 * given input.
 * <p>
 * The specified input should contain a representation of {@link uk.gov.gchq.gaffer.data.element.Edge}s
 * in a Gaffer graph. The PageRank algorithm is applied and a PageRank value is
 * determined for each of the vertices encountered. Note that the PageRank algorithm
 * will ignore any {@link uk.gov.gchq.gaffer.data.element.Entity} objects and values
 * will only be calculated for entities which share a vertex with an Edge.
 * <p>
 * The output of a PageRank operation is identical to the input type, but the input
 * data will be replaced with the results of the PageRank calculation.
 * <p>
 * Users specifying a PageRank operation MUST provide one of the following: <ul>
 * <li>tolerance - set the desired precision of the PageRank values. With this
 * setting PageRank will run continuously until convergence</li>
 * <li>maxIterations - set the maximum number of iterations before
 * returning</li> </ul>
 * <p>
 * Setting both of these values will result in an error.
 */
public class PageRank<T> implements InputOutput<T, T> {
    public static final double DEFAULT_RESET_PROBABILITY = 0.15d;

    public T input;
    public Map<String, String> options;

    /**
     * The desired precision of the PageRank values. Cannot
     * be used in conjunction with the maxIterations field.
     */
    protected Double tolerance;

    /**
     * The maximum number of iterations before terminating the operation. Cannot
     * be used in conjunction with the tolerance field.
     */
    protected Integer maxIterations;

    /**
     * The probability the algorithm starts from a node chosen uniformly at random
     * among all nodes in the network. (Also known as the damping factor.
     */
    protected Double resetProbability = DEFAULT_RESET_PROBABILITY;

    @Override
    public T getInput() {
        return input;
    }

    @Override
    public void setInput(final T input) {
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
    public TypeReference<T> getOutputTypeReference() {
        return TypeReferenceImpl.createExplicitT();
    }

    @Override
    public PageRank<T> shallowClone() {
        return new PageRank.Builder<T>()
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

    public interface PageRankBuilder<OP extends PageRank<T>, T, B extends InputOutput.Builder<OP, T, T, ?>>
            extends InputOutput.Builder<OP, T, T, B> {

        default B maxIterations(final Integer maxIterations) {
            _getOp().setMaxIterations(maxIterations);
            return _self();
        }

        default B tolerance(final Double tolerance) {
            _getOp().setTolerance(tolerance);
            return _self();
        }

        default B resetProbability(final Double resetProbability) {
            _getOp().setResetProbability(resetProbability);
            return _self();
        }
    }

    public static class Builder<T> extends BaseBuilder<PageRank<T>, Builder<T>>
            implements PageRankBuilder<PageRank<T>, T, Builder<T>> {

        public Builder() {
            super(new PageRank<>());
        }

        public Builder(final PageRank<T> pageRank) {
            super(pageRank);
        }
    }
}
