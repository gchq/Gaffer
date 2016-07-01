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

package gaffer.operation.impl.export;

import gaffer.commonutil.iterable.CloseableIterable;

/**
 * A <code>UpdateExport</code> allows the results of a previous operation in an
 * {@link gaffer.operation.OperationChain} to be added to a export, keyed on
 * an optional key. If a key is not provided the default key is 'ALL'.
 * The export is maintained per single {@link gaffer.operation.OperationChain} only.
 * It cannot be used across multiple separate operation requests.
 * So, it must be updated and fetched inside a single operation chain.
 *
 * @see FetchExporter
 * @see FetchExport
 */
public class UpdateExport extends ExportOperation<Iterable<Object>, CloseableIterable<Object>> {
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

    @Override
    public void setInput(final Iterable input) {
        super.setInput(input);
    }

    public static class Builder extends ExportOperation.Builder<UpdateExport, Iterable<Object>, CloseableIterable<Object>> {
        public Builder() {
            super(new UpdateExport());
        }

        @Override
        public Builder input(final Iterable input) {
            getOp().setInput(input);
            return this;
        }

        @Override
        public Builder key(final String key) {
            return (Builder) super.key(key);
        }

        @Override
        public Builder option(final String name, final String value) {
            return (Builder) super.option(name, value);
        }
    }
}
