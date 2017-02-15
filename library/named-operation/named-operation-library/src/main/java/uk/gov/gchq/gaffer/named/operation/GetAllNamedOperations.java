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

package uk.gov.gchq.gaffer.named.operation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.named.operation.serialisation.NamedOperationTypeReference;
import uk.gov.gchq.gaffer.operation.AbstractGetIterableOperation;

public class GetAllNamedOperations extends AbstractGetIterableOperation<Void, NamedOperation> {

    @JsonIgnore
    @Override
    public boolean isDeduplicate() {
        return deduplicate;
    }

    @Override
    protected TypeReference createOutputTypeReference() {
        return new NamedOperationTypeReference.IterableNamedOperation();
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends AbstractGetIterableOperation.BaseBuilder<GetAllNamedOperations, Void, NamedOperation, CHILD_CLASS> {
        public BaseBuilder() {
            super(new GetAllNamedOperations());
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}
