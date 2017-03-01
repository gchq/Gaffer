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

import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * An <code>JcsJobTracker</code> is an implementation of {@link JobTracker}.
 */
public class JcsJobTracker implements JobTracker {
    public static final String REGION = "jobTrackerRegion";
    private static final String CACHE_GROUP = "JobTracker";
    private JCS cache;

    @Override
    public void initialise(final String configPath) {
        if (null != configPath) {
            JCS.setConfigFilename(configPath);
        }

        try {
            cache = JCS.getInstance(REGION);
        } catch (final CacheException e) {
            // Try just the default region
            try {
                cache = JCS.getInstance("default");
            } catch (CacheException e2) {
                throw new RuntimeException("Unable to initialised the job tracker cache with config file: " + configPath, e);
            }
        }
    }

    @Override
    public void addOrUpdateJob(final JobDetail jobDetail, final User user) {
        validateJobDetail(jobDetail);

        try {
            cache.putInGroup(jobDetail.getJobId(), CACHE_GROUP, jobDetail);
        } catch (CacheException e) {
            throw new RuntimeException("Failed to add job to job tracker: " + jobDetail, e);
        }
    }

    @Override
    public JobDetail getJob(final String jobId, final User user) {
        return (JobDetail) cache.getFromGroup(jobId, CACHE_GROUP);
    }

    @Override
    public CloseableIterable<JobDetail> getAllJobs(final User user) {
        final Set<?> jobIds = cache.getGroupKeys(CACHE_GROUP);
        final List<JobDetail> jobs = new ArrayList<>(jobIds.size());
        jobIds.stream()
                .filter(jobId -> null != jobId)
                .forEach(jobId -> {
                    final JobDetail job = getJob(jobId.toString(), user);
                    if (null != job) {
                        jobs.add(job);
                    }
                });

        return new WrappedCloseableIterable<>(jobs);
    }

    @Override
    public void clear() {
        try {
            cache.clear();
        } catch (CacheException e) {
            throw new RuntimeException("Failed to clear the cache", e);
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
