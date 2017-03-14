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

package uk.gov.gchq.gaffer.operation.impl.get;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.IterableInput;
import uk.gov.gchq.gaffer.operation.IterableOutput;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Options;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import java.util.Collections;

/**
 * Restricts {@link uk.gov.gchq.gaffer.operation.impl.get.GetElements} to only return {@link uk.gov.gchq.gaffer.data.element.Edge}s.
 * See implementations of {@link GetEdges} for further details.
 *
 * @param <I_ITEM> the seed seed type
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetElements
 */
public class GetEdges<I_ITEM extends ElementSeed> extends GetElements<I_ITEM, Edge> {
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

    public static class Builder<I_ITEM extends ElementSeed> extends Operation.BaseBuilder<GetEdges<I_ITEM>, Builder<I_ITEM>>
            implements IterableInput.Builder<GetEdges<I_ITEM>, I_ITEM, Builder<I_ITEM>>,
            IterableOutput.Builder<GetEdges<I_ITEM>, Edge, Builder<I_ITEM>>,
            SeededGraphFilters.Builder<GetEdges<I_ITEM>, Builder<I_ITEM>>,
            SeedMatching.Builder<GetEdges<I_ITEM>, Builder<I_ITEM>>,
            Options.Builder<GetEdges<I_ITEM>, Builder<I_ITEM>> {
        public Builder() {
            super(new GetEdges<>());
        }
    }
}
