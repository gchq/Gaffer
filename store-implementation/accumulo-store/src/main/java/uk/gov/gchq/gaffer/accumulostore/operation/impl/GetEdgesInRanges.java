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

import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.IterableInput;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import java.util.Collections;

/**
 * Returns all {@link uk.gov.gchq.gaffer.data.element.Edge}'s between the provided
 * {@link uk.gov.gchq.gaffer.operation.data.ElementSeed}s.
 */
public class GetEdgesInRanges<I_ITEM extends Pair<? extends ElementSeed>> extends GetElementsInRanges<I_ITEM, Edge> {
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

    public static class Builder<I_ITEM extends Pair<? extends ElementSeed>>
            extends Operation.BaseBuilder<GetEdgesInRanges<I_ITEM>, Builder<I_ITEM>>
            implements IterableInput.Builder<GetEdgesInRanges<I_ITEM>, I_ITEM, Builder<I_ITEM>> {
        protected Builder() {
            super(new GetEdgesInRanges<>());
        }
    }
}
