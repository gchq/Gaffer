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
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.graph.AbstractSeededGraphGetIterable;
import java.util.Collections;

/**
 * This returns all {@link uk.gov.gchq.gaffer.data.element.Entity}'s between the provided
 * {@link uk.gov.gchq.gaffer.operation.data.ElementSeed}s.
 */
public class GetEntitiesInRanges<I_TYPE extends Pair<? extends ElementSeed>> extends GetElementsInRanges<I_TYPE, Entity> {
    @Override
    public void setView(final View view) {
        if (null != view && view.hasEdges()) {
            super.setView(new View.Builder()
                    .merge(view)
                    .edges(Collections.emptyMap())
                    .build());
        } else {
            super.setView(view);
        }
    }

    public abstract static class BaseBuilder<I_TYPE extends Pair<? extends ElementSeed>, CHILD_CLASS extends BaseBuilder<I_TYPE, ?>>
            extends AbstractSeededGraphGetIterable.BaseBuilder<GetEntitiesInRanges<I_TYPE>, I_TYPE, Entity, CHILD_CLASS> {
        public BaseBuilder() {
            super(new GetEntitiesInRanges<>());
        }
    }

    public static final class Builder<I_TYPE extends Pair<? extends ElementSeed>>
            extends BaseBuilder<I_TYPE, Builder<I_TYPE>> {

        @Override
        protected Builder<I_TYPE> self() {
            return this;
        }
    }
}
