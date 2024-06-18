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

package uk.gov.gchq.gaffer.tinkerpop.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.tinkerpop.process.traversal.step.util.GafferPopHasContainer;

/**
 * Custom GafferPop HasStep.
 * Uses {@link GafferPopHasContainer GafferPopHasContainers} rather than Gremlin {@link HasContainer HasContainers}.
 *
 * @see GafferPopHasContainer
 */
public class GafferPopHasStep<S extends Element> extends HasStep<S> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GafferPopHasStep.class);

    public GafferPopHasStep(final HasStep<S> originalHasStep) {
        super(originalHasStep.getTraversal());
        LOGGER.debug("Running custom HasStep on GafferPopGraph");

        originalHasStep.getHasContainers().forEach(this::addHasContainer);
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        // Convert Gremlin HasContainers to GafferPopHasContainers
        super.addHasContainer(new GafferPopHasContainer(hasContainer));
    }
}
