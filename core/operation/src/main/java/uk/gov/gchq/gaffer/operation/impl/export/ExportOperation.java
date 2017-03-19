/*
 * Copyright 2016-2017 Crown Copyright
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

import uk.gov.gchq.gaffer.operation.AbstractOperation;

public abstract class ExportOperation<INPUT, OUTPUT> extends AbstractOperation<INPUT, OUTPUT> {
    public static final String DEFAULT_KEY = "ALL";

    private String key = DEFAULT_KEY;

    public String getKey() {
        return key;
    }

    public void setKey(final String key) {
        if (null == key) {
            this.key = DEFAULT_KEY;
        } else {
            this.key = key;
        }
    }

    public abstract static class BaseBuilder<OP_TYPE extends ExportOperation<INPUT, OUTPUT>,
            INPUT,
            OUTPUT,
            CHILD_CLASS extends BaseBuilder<OP_TYPE, INPUT, OUTPUT, ?>>
            extends AbstractOperation.BaseBuilder<OP_TYPE, INPUT, OUTPUT, CHILD_CLASS> {
        public BaseBuilder(final OP_TYPE exportOperation) {
            super(exportOperation);
        }

        public CHILD_CLASS key(final String key) {
            getOp().setKey(key);
            return self();
        }
    }
}
