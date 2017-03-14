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
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.IterableOutput;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Options;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import java.util.Collections;

/**
 * Restricts {@link GetAllElements} to only return entities.
 */
public class GetAllEntities extends GetAllElements<Entity> {
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

    public static class Builder extends Operation.BaseBuilder<GetAllEntities, Builder>
            implements IterableOutput.Builder<GetAllEntities, Entity, Builder>,
            GraphFilters.Builder<GetAllEntities, Builder>,
            Options.Builder<GetAllEntities, Builder> {
        public Builder() {
            super(new GetAllEntities());
        }
    }
}
