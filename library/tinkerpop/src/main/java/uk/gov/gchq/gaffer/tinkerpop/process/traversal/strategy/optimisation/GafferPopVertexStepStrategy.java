/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.process.traversal.strategy.optimisation;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.tinkerpop.process.traversal.step.GafferPopVertexStep;
import uk.gov.gchq.gaffer.tinkerpop.process.traversal.step.LazyFoldStep;

/**
 * Optimisation strategy to reduce the number of Gaffer operations performed.
 * Replaces the VertexStep so they operate on multiple vertices at once.
 */
public final class GafferPopVertexStepStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(GafferPopVertexStepStrategy.class);
    private static final GafferPopVertexStepStrategy INSTANCE = new GafferPopVertexStepStrategy();

    private GafferPopVertexStepStrategy() {
    }

    @Override
    public void apply(final Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfClass(VertexStep.class, traversal).forEach(originalVertexStep -> {
            LOGGER.debug("Inserting FoldStep and replacing VertexStep");

            // Replace vertex step
            final GafferPopVertexStep<? extends Element> listVertexStep = new GafferPopVertexStep<>(
                    originalVertexStep);
            TraversalHelper.replaceStep(originalVertexStep, listVertexStep, traversal);

            // Add in a fold step before the new VertexStep so that the input is the list of
            // all vertices
            LazyFoldStep<Vertex> lazyFoldStep = new LazyFoldStep<>(originalVertexStep.getTraversal());
            TraversalHelper.insertBeforeStep(lazyFoldStep, listVertexStep, traversal);
        });
    }

    public static GafferPopVertexStepStrategy instance() {
        return INSTANCE;
    }

}
