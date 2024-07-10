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
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import uk.gov.gchq.gaffer.tinkerpop.process.traversal.step.GafferPopHasStep;

public final class GafferPopHasStepStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {
    private static final GafferPopHasStepStrategy INSTANCE = new GafferPopHasStepStrategy();

    private GafferPopHasStepStrategy() {
    }

    @Override
    public void apply(final Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfClass(HasStep.class, traversal).forEach(originalHasStep -> {
            // Replace the current HasStep with a GafferPopHasStep
            final GafferPopHasStep<?> gafferPopHasStep = new GafferPopHasStep<>(originalHasStep);
            TraversalHelper.replaceStep(originalHasStep, gafferPopHasStep, traversal);
        });
    }

    public static GafferPopHasStepStrategy instance() {
        return INSTANCE;
    }
}
