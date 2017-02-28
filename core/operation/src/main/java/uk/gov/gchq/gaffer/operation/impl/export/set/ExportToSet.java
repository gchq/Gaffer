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

package uk.gov.gchq.gaffer.operation.impl.export.set;

import uk.gov.gchq.gaffer.operation.impl.export.Export;

/**
 * An <code>ExportToSet</code> Export operation exports results to a Set.
 * This Set export is maintained per single Job or {@link uk.gov.gchq.gaffer.operation.OperationChain} only.
 * It cannot be used across multiple separate operation requests.
 * So ExportToSet and GetSetExport must be used inside a single operation chain.
 */
public class ExportToSet extends Export {
    public abstract static class BaseBuilder<EXPORT extends ExportToSet, CHILD_CLASS extends BaseBuilder<EXPORT, CHILD_CLASS>>
            extends Export.BaseBuilder<ExportToSet, CHILD_CLASS> {
        public BaseBuilder(final ExportToSet export) {
            super(export);
        }
    }

    public static final class Builder extends BaseBuilder<ExportToSet, Builder> {
        public Builder() {
            super(new ExportToSet());
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
