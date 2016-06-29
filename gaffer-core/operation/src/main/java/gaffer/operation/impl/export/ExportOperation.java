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

import gaffer.operation.AbstractOperation;
import gaffer.util.ExportUtil;

public abstract class ExportOperation<INPUT, OUTPUT> extends AbstractOperation<INPUT, OUTPUT> {
    public static final String DEFAULT_KEY = "ALL";
    private String key;

    /**
     * Constructs an <code>ExportOperation</code> with the key set to 'ALL'.
     */
    public ExportOperation() {
        this(DEFAULT_KEY);
    }

    /**
     * Constructs an <code>UpdateExport</code> with the provided key.
     *
     * @param key the key to use to store the results in the export.
     */
    public ExportOperation(final String key) {
        setKey(key);
    }

    public String getKey() {
        return key;
    }

    public void setKey(final String key) {
        ExportUtil.validateKey(key);
        this.key = key;
    }

    public static class Builder<OP_TYPE extends ExportOperation<INPUT, OUTPUT>, INPUT, OUTPUT>
            extends AbstractOperation.Builder<OP_TYPE, INPUT, OUTPUT> {
        public Builder(final OP_TYPE exportOperation) {
            super(exportOperation);
        }

        public Builder key(final String key) {
            getOp().setKey(key);
            return this;
        }

        @Override
        public Builder<OP_TYPE, INPUT, OUTPUT> option(final String name, final String value) {
            return (Builder<OP_TYPE, INPUT, OUTPUT>) super.option(name, value);
        }
    }
}
