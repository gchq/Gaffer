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

import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import java.util.Collections;

/**
 * Restricts {@link uk.gov.gchq.gaffer.operation.impl.get.GetElements} to only return {@link uk.gov.gchq.gaffer.data.element.Entity}s.
 * See implementations of {@link GetEntities} for further details.
 *
 * @param <ID> the seed seed type
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetElements
 */
public class GetEntities<ID extends ElementId> extends GetElements<ID, Entity> {
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

    public abstract static class BaseBuilder<ID extends ElementId,
            CHILD_CLASS extends BaseBuilder<ID, ?>>
            extends GetElements.BaseBuilder<GetEntities<ID>, ID, Entity, CHILD_CLASS> {
        protected BaseBuilder() {
            super(new GetEntities<>());
        }

        protected BaseBuilder(final GetEntities<ID> op) {
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
