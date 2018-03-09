/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.export;

/**
 * A {@code GetExport} is an {@link uk.gov.gchq.gaffer.operation.Operation} to
 * retrieve the details of an {@link Export} operation.
 */
public interface GetExport extends Export {
    String getJobId();

    void setJobId(final String jobId);

    interface Builder<OP extends GetExport, B extends Builder<OP, ?>>
            extends Export.Builder<OP, B> {
        default B jobId(final String jobId) {
            _getOp().setJobId(jobId);
            return _self();
        }
    }
}
