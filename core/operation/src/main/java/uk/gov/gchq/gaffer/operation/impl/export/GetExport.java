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

import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.operation.VoidInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

/**
 * A <code>GetExport</code> fetches an export {@link Iterable} for a
 * provided jobId and key. If a key is not provided the default key is 'ALL'.
 */
public abstract class GetExport extends ExportOperation<Void, CloseableIterable<?>> implements VoidInput<CloseableIterable<?>> {
    private String jobId;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(final String jobId) {
        this.jobId = jobId;
    }

    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.CloseableIterableObj();
    }

    public abstract static class BaseBuilder<EXPORT extends GetExport, CHILD_CLASS extends BaseBuilder<EXPORT, ?>>
            extends ExportOperation.BaseBuilder<EXPORT, Void, CloseableIterable<?>, CHILD_CLASS> {
        public BaseBuilder(final EXPORT export) {
            super(export);
        }

        public CHILD_CLASS jobId(final String jobId) {
            getOp().setJobId(jobId);
            return self();
        }
    }
}
