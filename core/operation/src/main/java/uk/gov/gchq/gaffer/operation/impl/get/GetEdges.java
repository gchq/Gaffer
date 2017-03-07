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
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import java.util.Collections;

/**
 * Restricts {@link uk.gov.gchq.gaffer.operation.impl.get.GetElements} to only return {@link uk.gov.gchq.gaffer.data.element.Edge}s.
 * See implementations of {@link GetEdges} for further details.
 *
 * @param <ID> the id type
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetElements
 */
public class GetEdges<ID extends ElementId> extends GetElements<ID, Edge> {
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

    public abstract static class BaseBuilder<ID extends ElementId, CHILD_CLASS extends BaseBuilder<ID, ?>>
            extends GetElements.BaseBuilder<GetEdges<ID>, ID, Edge, CHILD_CLASS> {

        public BaseBuilder() {
            super(new GetEdges<>());
        }

        protected BaseBuilder(final GetEdges<ID> op) {
            super(op);
        }
    }

    public static final class Builder<ID extends ElementId> extends BaseBuilder<ID, Builder<ID>> {

        @Override
        protected Builder<ID> self() {
            return this;
        }
    }
}
