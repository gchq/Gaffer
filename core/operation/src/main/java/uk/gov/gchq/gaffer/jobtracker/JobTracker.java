/*
 * Copyright 2016-2021 Crown Copyright
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

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

/**
 * A {@code JobTracker} is an entry in a Gaffer cache service which is used to store
 * details of jobs submitted to the graph.
 */
public class JobTracker {

    private String cacheName;

    public static final String CACHE_SERVICE_NAME_PREFIX = "JobTracker";

    public JobTracker() {
        this(null);
    }

    public JobTracker(final String cacheNameSuffix) {
        cacheName = String.format("%s%s", CACHE_SERVICE_NAME_PREFIX, nonNull(cacheNameSuffix) ? "_" + cacheNameSuffix.toLowerCase() : "");
    }

    /**
     * Add or update the job details relating to a job in the job tracker cache.
     *
     * @param jobDetail the job details to update
     * @param user      the user making the request
     */
    public void addOrUpdateJob(final JobDetail jobDetail, final User user) {
        validateJobDetail(jobDetail);
        try {
            CacheServiceLoader.getService().putInCache(cacheName, jobDetail.getJobId(), jobDetail);
        } catch (final CacheOperationException e) {
            throw new RuntimeException("Failed to add jobDetail " + jobDetail.toString() + " to the cache", e);
        }
    }

    /**
     * Get the details of a specific job.
     *
     * @param jobId the ID of the job to lookup
     * @param user  the user making the request to the job tracker
     * @return the {@link JobDetail} object for the requested job
     */
    public JobDetail getJob(final String jobId, final User user) {
        return CacheServiceLoader.getService().getFromCache(cacheName, jobId);
    }

    /**
     * Get all jobs from the job tracker cache.
     *
     * @param user the user making the request to the job tracker
     * @return a {@link CloseableIterable} containing all of the job details
     */
    public CloseableIterable<JobDetail> getAllJobs(final User user) {

        return getAllJobsMatching(user, jd -> true);
    }

    /**
     * Get all scheduled jobs from the job tracker cache.
     *
     * @return a {@link CloseableIterable} containing all of the scheduled job details
     */
    public CloseableIterable<JobDetail> getAllScheduledJobs() {

        return getAllJobsMatching(new User(), jd -> jd.getStatus().equals(JobStatus.SCHEDULED_PARENT));
    }

    private CloseableIterable<JobDetail> getAllJobsMatching(final User user, final Predicate<JobDetail> jobDetailPredicate) {

        final Set<String> jobIds = CacheServiceLoader.getService().getAllKeysFromCache(cacheName);
        final List<JobDetail> jobs = jobIds.stream()
                .filter(Objects::nonNull)
                .map(jobId -> getJob(jobId, user))
                .filter(Objects::nonNull)
                .filter(jobDetailPredicate)
                .collect(Collectors.toList());

        return new WrappedCloseableIterable<>(jobs);
    }

    /**
     * Clear the job tracker cache.
     */
    public void clear() {
        try {
            CacheServiceLoader.getService().clearCache(cacheName);
        } catch (final CacheOperationException e) {
            throw new RuntimeException("Failed to clear job tracker cache", e);
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

    public String getCacheName() {
        return cacheName;
    }
}
