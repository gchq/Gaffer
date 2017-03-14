/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.operation.impl;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.IterableInput;
import uk.gov.gchq.gaffer.operation.IterableInputB;
import uk.gov.gchq.gaffer.operation.IterableOutput;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Options;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import java.util.Collections;

/**
 * Given two sets of {@link uk.gov.gchq.gaffer.operation.data.EntitySeed}s, called A and B,
 * this retrieves all {@link uk.gov.gchq.gaffer.data.element.Edge}s where one end is in set
 * A and the other is in set B.
 */
public class GetEdgesBetweenSets extends GetElementsBetweenSets<Edge> {
    @Override
    public void setView(final View view) {
        if (null != view && view.hasEntities()) {
            super.setView(new View.Builder()
                    .merge(view)
                    .entities(Collections.emptyMap())
                    .build());
        } else {
            super.setView(view);
        }
    }

    public static class Builder extends Operation.BaseBuilder<GetEdgesBetweenSets, Builder>
            implements IterableInput.Builder<GetEdgesBetweenSets, EntitySeed, Builder>,
            IterableInputB.Builder<GetEdgesBetweenSets, EntitySeed, Builder>,
            IterableOutput.Builder<GetEdgesBetweenSets, Edge, Builder>,
            SeededGraphFilters.Builder<GetEdgesBetweenSets, Builder>,
            SeedMatching.Builder<GetEdgesBetweenSets, Builder>,
            Options.Builder<GetEdgesBetweenSets, Builder> {
        public Builder() {
            super(new GetEdgesBetweenSets());
        }
    }
}
