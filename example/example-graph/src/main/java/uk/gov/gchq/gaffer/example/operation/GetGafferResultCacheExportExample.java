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
package uk.gov.gchq.gaffer.example.operation;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.export.GetExports;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.ExportToGafferResultCache;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.GetGafferResultCacheExport;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEdges;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEntities;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import java.util.Map;

public class GetGafferResultCacheExportExample extends OperationExample {
    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "it will be set prior to use")
    private JobDetail jobDetail;

    public static void main(final String[] args) throws OperationException {
        new GetGafferResultCacheExportExample().run();
    }

    public GetGafferResultCacheExportExample() {
        super(GetGafferResultCacheExport.class);
    }

    @Override
    public void runExamples() {
        simpleExportAndGet();
        exportAndGetJobDetails();
        getExport();
        exportMultipleResultsToGafferResultCacheAndGetAllResults();
    }

    public CloseableIterable<?> simpleExportAndGet() {
        // ---------------------------------------------------------
        final OperationChain<CloseableIterable<?>> opChain = new OperationChain.Builder()
                .first(new GetAllEdges())
                .then(new ExportToGafferResultCache())
                .then(new GetGafferResultCacheExport())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain);
    }

    public JobDetail exportAndGetJobDetails() {
        // ---------------------------------------------------------
        final OperationChain<JobDetail> exportOpChain = new OperationChain.Builder()
                .first(new GetAllEdges())
                .then(new ExportToGafferResultCache())
                .then(new GetJobDetails())
                .build();
        // ---------------------------------------------------------

        jobDetail = runExample(exportOpChain);
        return jobDetail;
    }

    public CloseableIterable<?> getExport() {
        // ---------------------------------------------------------
        final OperationChain<CloseableIterable<?>> opChain = new OperationChain.Builder()
                .first(new GetGafferResultCacheExport.Builder()
                        .jobId(jobDetail.getJobId())
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain);
    }

    public Map<String, CloseableIterable<?>> exportMultipleResultsToGafferResultCacheAndGetAllResults() {
        // ---------------------------------------------------------
        final OperationChain<Map<String, CloseableIterable<?>>> opChain = new OperationChain.Builder()
                .first(new GetAllEdges())
                .then(new ExportToGafferResultCache.Builder()
                        .key("edges")
                        .build())
                .then(new GetAllEntities())
                .then(new ExportToGafferResultCache.Builder()
                        .key("entities")
                        .build())
                .then(new GetExports.Builder()
                        .exports(new GetGafferResultCacheExport.Builder()
                                        .key("edges")
                                        .build(),
                                new GetGafferResultCacheExport.Builder()
                                        .key("entities")
                                        .build())
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain);
    }
}
