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

import gaffer.data.element.Edge;
import gaffer.data.elementdefinition.view.View;

/**
 * Restricts {@link GetAllElements} to only return edges.
 */
public class GetAllEdges extends GetAllElements<Edge> {
    public GetAllEdges() {
        super();
    }

    public GetAllEdges(final View view) {
        super(view);
    }

    public GetAllEdges(final GetAllEdges operation) {
        super(operation);
    }

    @Override
    public SeedMatchingType getSeedMatching() {
        return SeedMatchingType.EQUAL;
    }

    @Override
    public boolean isIncludeEntities() {
        return false;
    }

    @Override
    public void setIncludeEntities(final boolean includeEntities) {
        if (includeEntities) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " does not support including entities");
        }
    }

    @Override
    public void setIncludeEdges(final IncludeEdgeType includeEdges) {
        if (IncludeEdgeType.NONE == includeEdges) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " requires edges to be included");
        }

        super.setIncludeEdges(includeEdges);
    }

    public static class Builder extends GetAllElements.Builder<Edge> {
        public Builder() {
            this(new GetAllEdges());
        }

        public Builder(final GetAllEdges op) {
            super(op);
        }

        @Override
        public Builder includeEdges(final IncludeEdgeType includeEdgeType) {
            super.includeEdges(includeEdgeType);
            return this;
        }

        @Override
        public Builder summarise(final boolean summarise) {
            super.summarise(summarise);
            return this;
        }

        @Override
        public Builder deduplicate(final boolean deduplicate) {
            return (Builder) super.deduplicate(deduplicate);
        }

        @Override
        public Builder populateProperties(final boolean populateProperties) {
            super.populateProperties(populateProperties);
            return this;
        }

        @Override
        public Builder view(final View view) {
            super.view(view);
            return this;
        }

        @Override
        public Builder option(final String name, final String value) {
            super.option(name, value);
            return this;
        }

        @Override
        public GetAllEdges build() {
            return (GetAllEdges) super.build();
        }
    }
}
