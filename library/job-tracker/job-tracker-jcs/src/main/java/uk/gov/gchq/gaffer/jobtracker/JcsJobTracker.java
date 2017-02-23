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

package uk.gov.gchq.gaffer.jobtracker;

import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.TransformIterable;
import uk.gov.gchq.gaffer.user.User;

/**
 * An <code>JcsJobTracker</code> is an implementation of {@link JobTracker}.
 */
public class JcsJobTracker implements JobTracker {
    public static final String REGION = "oneWeek";
    private static final String CACHE_GROUP = "JobTracker";
    private JCS cache;

    /**
     * Default constructor which creates a new instance of a JCS object using the default configuration found in the
     * cache.ccf file
     *
     * @throws CacheException thrown when the default profile is not available or the cache.ccf file is missing
     */
    public JcsJobTracker() throws CacheException {
        cache = JCS.getInstance(REGION);
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
        return new TransformIterable<String, JobDetail>(cache.getGroupKeys(CACHE_GROUP)) {
            @Override
            protected JobDetail transform(final String jobId) {
                return getJob(jobId, user);
            }
        };
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
