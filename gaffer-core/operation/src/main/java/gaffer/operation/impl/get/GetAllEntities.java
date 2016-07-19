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
import gaffer.data.elementdefinition.view.View;

/**
 * Restricts {@link GetAllElements} to only return entities.
 */
public class GetAllEntities extends GetAllElements<Entity> {
    public GetAllEntities() {
        super();
    }

    public GetAllEntities(final View view) {
        super(view);
    }

    public GetAllEntities(final GetAllEntities operation) {
        super(operation);
    }

    @Override
    public SeedMatchingType getSeedMatching() {
        return SeedMatchingType.EQUAL;
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

    public static class Builder extends GetAllElements.Builder<Entity> {
        public Builder() {
            this(new GetAllEntities());
        }

        public Builder(final GetAllEntities op) {
            super(op);
        }

        @Override
        public Builder includeEntities(final boolean includeEntities) {
            super.includeEntities(includeEntities);
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
        public GetAllEntities build() {
            return (GetAllEntities) super.build();
        }
    }
}
