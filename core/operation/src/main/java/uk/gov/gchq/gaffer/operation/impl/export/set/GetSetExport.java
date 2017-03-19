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

import uk.gov.gchq.gaffer.operation.impl.export.GetExport;

/**
 * An <code>GetSetExport</code> GetExport operation gets exported Set results.
 * The Set export is maintained per single Job or {@link uk.gov.gchq.gaffer.operation.OperationChain} only.
 * It cannot be used across multiple separate operation requests.
 * So ExportToSet and GetSetExport must be used inside a single operation chain.
 */
public class GetSetExport extends GetExport {
    private int start = 0;
    private Integer end = null;

    public int getStart() {
        return start;
    }

    public void setStart(final int start) {
        this.start = start;
    }

    public Integer getEnd() {
        return end;
    }

    public void setEnd(final Integer end) {
        this.end = end;
    }

    public abstract static class BaseBuilder<EXPORT extends GetSetExport, CHILD_CLASS extends BaseBuilder<EXPORT, CHILD_CLASS>>
            extends GetExport.BaseBuilder<GetSetExport, CHILD_CLASS> {
        public BaseBuilder(final GetSetExport export) {
            super(export);
        }

        public CHILD_CLASS start(final int start) {
            getOp().setStart(start);
            return self();
        }

        public CHILD_CLASS end(final Integer end) {
            getOp().setEnd(end);
            return self();
        }
    }

    public static final class Builder extends BaseBuilder<GetSetExport, Builder> {
        public Builder() {
            super(new GetSetExport());
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
