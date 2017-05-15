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

package uk.gov.gchq.gaffer.jobtracker;


import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class JobTracker {

    private static final String CACHE_NAME = "JobTracker";

    public void addOrUpdateJob(final JobDetail jobDetail, final User user) {
        validateJobDetail(jobDetail);

        try {
            CacheServiceLoader.getService().putInCache(CACHE_NAME, jobDetail.getJobId(), jobDetail);
        } catch (CacheOperationException e) {
            throw new RuntimeException("Failed to add jobDetail " + jobDetail.toString() + " to the uk.gov.gchq.gaffer.cache", e);
        }
    }


    public JobDetail getJob(final String jobId, final User user) {
        return CacheServiceLoader.getService().getFromCache(CACHE_NAME, jobId);
    }

    public CloseableIterable<JobDetail> getAllJobs(final User user) {
        Set<String> jobIds = CacheServiceLoader.getService().getAllKeysFromCache(CACHE_NAME);
        final List<JobDetail> jobs = new ArrayList<>(jobIds.size());
        jobIds.stream()
                .filter(jobId -> null != jobId)
                .forEach(jobId -> {
                    final JobDetail job = getJob(jobId, user);
                    if (null != job) {
                        jobs.add(job);
                    }
                });

        return new WrappedCloseableIterable<>(jobs);
    }

    public void clear() {
        try {
            CacheServiceLoader.getService().clearCache(CACHE_NAME);
        } catch (CacheOperationException e) {
            throw new RuntimeException("Failed to clear job tracker uk.gov.gchq.gaffer.cache", e);
        }
    }

    private void validateJobDetail(final JobDetail jobDetail) {
        if (null == jobDetail) {
            throw new IllegalArgumentException("JobDetail is required");
        }

        if (null == jobDetail.getJobId() || jobDetail.getJobId().isEmpty()) {
            throw new IllegalArgumentException("jobId is required");
        }
    }

}
