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

public class GafferPopGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    private static final GafferPopGraphStepStrategy INSTANCE = new GafferPopGraphStepStrategy();

    private GafferPopGraphStepStrategy() {
    }

    @Override
    public void apply(Admin<?, ?> traversal) {

        TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(originalGraphStep -> {
            // Replace the current step with a GafferPopGraphStep
            final GafferPopGraphStep<?, ?> gafferPopGraphStep = new GafferPopGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(originalGraphStep, gafferPopGraphStep, traversal);
            Step<?, ?> currentStep = gafferPopGraphStep.getNextStep();

            // Loop over rest of the Steps and capture any HasSteps to add the hasContainers to the GafferPopGraphStep
            // this is so the filtering those steps do instead happens in the GafferPopGraphStep
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
