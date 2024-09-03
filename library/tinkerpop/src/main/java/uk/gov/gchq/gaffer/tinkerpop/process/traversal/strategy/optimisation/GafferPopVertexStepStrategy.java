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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.tinkerpop.process.traversal.step.GafferPopVertexStep;

public final class GafferPopVertexStepStrategy
    extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
    implements TraversalStrategy.ProviderOptimizationStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(GafferPopVertexStepStrategy.class);
  private static final GafferPopVertexStepStrategy INSTANCE = new GafferPopVertexStepStrategy();

  private GafferPopVertexStepStrategy() {

  }

  @Override
  public void apply(final Admin<?, ?> traversal) {
    TraversalHelper.getStepsOfClass(VertexStep.class, traversal).forEach(originalGraphStep -> {
      final GafferPopVertexStep<?> gafferPopVertexStep = new GafferPopVertexStep<>(originalGraphStep);

      LOGGER.debug("Inserting FoldStep and replacing VertexStep");
      TraversalHelper.replaceStep(originalGraphStep, gafferPopVertexStep, traversal);
      TraversalHelper.insertBeforeStep(new FoldStep<>(originalGraphStep.getTraversal()), gafferPopVertexStep,
          traversal);
    });
  }

  public static GafferPopVertexStepStrategy instance() {
    return INSTANCE;
  }

}
