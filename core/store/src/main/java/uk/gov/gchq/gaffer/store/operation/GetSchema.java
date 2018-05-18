/*
 * Copyright 2017-2018 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.AbstractOperation;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.TypeReferenceStoreImpl;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

/**
 * A {@code GetSchema} is an {@link uk.gov.gchq.gaffer.operation.Operation} which
 * returns either the compact or full {@link Schema} for a Gaffer {@link uk.gov.gchq.gaffer.store.Store}.
 */
@JsonPropertyOrder(value = {"class"}, alphabetic = true)
@Since("1.1.0")
@Summary("Gets the Schema of a Graph")
public class GetSchema extends AbstractOperation<GetSchema> implements Output<Schema> {
    private boolean compact = false;

    public boolean isCompact() {
        return compact;
    }

    public void setCompact(final boolean compact) {
        this.compact(compact);
    }

    @Override
    public GetSchema shallowClone() throws CloneFailedException {
        return new GetSchema()
                .compact(compact)
                .options(getOptions());
    }

    @Override
    public TypeReference<Schema> getOutputTypeReference() {
        return new TypeReferenceStoreImpl.Schema();
    }

    public GetSchema compact(final boolean compact) {
        this.compact = compact;
        return this;
    }

}
