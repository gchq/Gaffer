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

package uk.gov.gchq.gaffer.operation.impl.export;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

/**
 * A <code>UpdateExport</code> allows the results of a previous operation in an
 * {@link uk.gov.gchq.gaffer.operation.OperationChain} to be added to a export, keyed on
 * an optional key. If a key is not provided the default key is 'ALL'.
 * The export is maintained per single {@link uk.gov.gchq.gaffer.operation.OperationChain} only.
 * It cannot be used across multiple separate operation requests.
 * So, it must be updated and fetched inside a single operation chain.
 *
 * @see FetchExporter
 * @see FetchExport
 */
public class UpdateExport extends ExportOperation<CloseableIterable<Object>, CloseableIterable<Object>> {
    /**
     * Constructs an <code>UpdateExport</code> with the key set to 'ALL'.
     */
    public UpdateExport() {
        super();
    }

    /**
     * Constructs an <code>UpdateExport</code> with the provided key.
     *
     * @param key the key to use to store the results in the export.
     */
    public UpdateExport(final String key) {
        super(key);
    }

    @JsonIgnore
    @Override
    public void setInput(final CloseableIterable input) {
        super.setInput(input);
    }

    public void setInput(final Iterable input) {
        super.setInput(new WrappedCloseableIterable<Object>(input));
    }

    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.CloseableIterableObj();
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends ExportOperation.BaseBuilder<UpdateExport, CloseableIterable<Object>, CloseableIterable<Object>, CHILD_CLASS> {
        public BaseBuilder() {
            super(new UpdateExport());
        }

        public CHILD_CLASS input(final Iterable input) {
            getOp().setInput(input);
            return self();
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}
