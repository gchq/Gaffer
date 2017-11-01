/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.TypeReferenceStoreImpl;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Map;

/**
 * A {@code GetSchema} is an {@link uk.gov.gchq.gaffer.operation.Operation} which
 * returns either the compact or full {@link Schema} for a Gaffer {@link uk.gov.gchq.gaffer.store.Store}.
 */
public class GetSchema implements Output<Schema> {
    private Map<String, String> options;
    private boolean compact = false;

    public boolean isCompact() {
        return compact;
    }

    public void setCompact(final boolean compact) {
        this.compact = compact;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public GetSchema shallowClone() throws CloneFailedException {
        return new Builder()
                .compact(compact)
                .options(options)
                .build();
    }

    @Override
    public TypeReference<Schema> getOutputTypeReference() {
        return new TypeReferenceStoreImpl.Schema();
    }

    public static class Builder extends BaseBuilder<GetSchema, Builder>
            implements Output.Builder<GetSchema, Schema, Builder> {
        public Builder() {
            super(new GetSchema());
        }

        public Builder compact(final boolean compact) {
            _getOp().setCompact(compact);
            return this;
        }
    }
}
