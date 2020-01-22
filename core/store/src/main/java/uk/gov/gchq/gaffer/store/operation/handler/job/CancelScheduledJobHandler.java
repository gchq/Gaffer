/*
 * Copyright 2019-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.job;

import uk.gov.gchq.gaffer.jobtracker.JobStatus;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.job.CancelScheduledJob;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

public class CancelScheduledJobHandler implements OperationHandler<CancelScheduledJob> {
    @Override
    public Void doOperation(final CancelScheduledJob operation, final Context context, final Store store) throws OperationException {
        if (null == store.getJobTracker()) {
            throw new OperationException("JobTracker not enabled");
        }
        if (null == operation.getJobId()) {
            throw new OperationException("job id must be specified");
        }

        if (store.getJobTracker().getJob(operation.getJobId(), context.getUser()).getStatus().equals(JobStatus.SCHEDULED_PARENT)) {
            store.getJobTracker().getJob(operation.getJobId(), context.getUser()).setStatus(JobStatus.CANCELLED);
        } else {
            throw new OperationException("Job with jobId: " + operation.getJobId() + " is not a scheduled job and cannot be cancelled.");
        }
        return null;
    }
}
