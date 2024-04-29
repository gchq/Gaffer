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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import uk.gov.gchq.gaffer.tinkerpop.process.traversal.step.GafferPopGraphStep;

/**
 * The {@link GraphStep} strategy for GafferPop, this will replace the default
 * {@link GraphStep} of a query to add Gaffer optimisations. Such as gathering
 * any {@link HasStep}s so that a Gaffer View can be constructed for the query.
 *
 * <pre>
 * g.V().hasLabel()    // replaced by GafferPopGraphStep
 * g.E().hasLabel()    // replaced by GafferPopGraphStep
 * </pre>
 */
public final class GafferPopGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    private static final GafferPopGraphStepStrategy INSTANCE = new GafferPopGraphStepStrategy();

    private GafferPopGraphStepStrategy() {
    }

    @Override
    public void apply(final Admin<?, ?> traversal) {

        TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(originalGraphStep -> {
            // Replace the current GraphStep with a GafferPopGraphStep
            final GafferPopGraphStep<?, ?> gafferPopGraphStep = new GafferPopGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(originalGraphStep, gafferPopGraphStep, traversal);

            // Loop over rest of the Steps and capture any HasSteps to add the hasContainers to the GafferPopGraphStep
            // this is so the filtering those steps would do instead happens in the GafferPopGraphStep.
            // Note we only want to capture HasSteps that act on the initial GraphStep e.g
            // 'g.V().hasLabel("label")' not 'g.V().out().hasLabel("label")'
            Step<?, ?> currentStep = gafferPopGraphStep.getNextStep();
            while (currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep) {
                if (currentStep instanceof HasStep) {
                    ((HasContainerHolder) currentStep).getHasContainers().forEach(hasContainer -> {
                        if (!GraphStep.processHasContainerIds(gafferPopGraphStep, hasContainer)) {
                            gafferPopGraphStep.addHasContainer(hasContainer);
                        }
                    });
                    TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                    traversal.removeStep(currentStep);
                }
                currentStep = currentStep.getNextStep();
            }
        });
    }

    public static GafferPopGraphStepStrategy instance() {
        return INSTANCE;
    }
}