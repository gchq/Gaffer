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

package gaffer.operation.impl.get;

import gaffer.data.element.Entity;
import gaffer.operation.data.ElementSeed;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.GetOperation;

/**
 * Restricts {@link gaffer.operation.impl.get.GetElements} to only return {@link gaffer.data.element.Entity}s.
 * See implementations of {@link GetEntities} for further details.
 *
 * @param <SEED_TYPE> the seed seed type
 * @see gaffer.operation.impl.get.GetElements
 */
public abstract class GetEntities<SEED_TYPE extends ElementSeed> extends GetElements<SEED_TYPE, Entity> {
    public GetEntities() {
        super();
    }

    public GetEntities(final Iterable<SEED_TYPE> seeds) {
        super(seeds);
    }

    public GetEntities(final View view) {
        super(view);
    }

    public GetEntities(final View view, final Iterable<SEED_TYPE> seeds) {
        super(view, seeds);
    }

    public GetEntities(final GetOperation<SEED_TYPE, ?> operation) {
        super(operation);
    }

    @Override
    public boolean isIncludeEntities() {
        return true;
    }

    @Override
    public void setIncludeEntities(final boolean includeEntities) {
        if (!includeEntities) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " requires entities to be included");
        }
    }

    @Override
    public IncludeEdgeType getIncludeEdges() {
        return IncludeEdgeType.NONE;
    }

    @Override
    public void setIncludeEdges(final IncludeEdgeType includeEdges) {
        if (IncludeEdgeType.NONE != includeEdges) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " does not support including edges");
        }
    }

    public static class Builder<OP_TYPE extends GetEntities<SEED_TYPE>, SEED_TYPE extends ElementSeed>
            extends GetElements.Builder<OP_TYPE, SEED_TYPE, Entity> {
        protected Builder(final OP_TYPE op) {
            super(op);
        }
    }
}
