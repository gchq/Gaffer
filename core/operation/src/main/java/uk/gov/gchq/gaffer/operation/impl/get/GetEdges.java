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
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import java.util.Collections;

/**
 * Restricts {@link uk.gov.gchq.gaffer.operation.impl.get.GetElements} to only return {@link uk.gov.gchq.gaffer.data.element.Edge}s.
 * See implementations of {@link GetEdges} for further details.
 *
 * @param <SEED_TYPE> the seed seed type
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetElements
 */
public class GetEdges<SEED_TYPE extends ElementSeed> extends GetElements<SEED_TYPE, Edge> {
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

    public abstract static class BaseBuilder<SEED_TYPE extends ElementSeed, CHILD_CLASS extends BaseBuilder<SEED_TYPE, ?>>
            extends GetElements.BaseBuilder<GetEdges<SEED_TYPE>, SEED_TYPE, Edge, CHILD_CLASS> {

        public BaseBuilder() {
            super(new GetEdges<>());
        }

        protected BaseBuilder(final GetEdges<SEED_TYPE> op) {
            super(op);
        }
    }

    public static final class Builder<SEED_TYPE extends ElementSeed> extends BaseBuilder<SEED_TYPE, Builder<SEED_TYPE>> {

        @Override
        protected Builder<SEED_TYPE> self() {
            return this;
        }
    }
}
