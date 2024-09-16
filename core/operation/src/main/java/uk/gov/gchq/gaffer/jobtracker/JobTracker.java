/*
 * Copyright 2016-2024 Crown Copyright
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.cache.Cache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.user.User;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

/**
 * A {@code JobTracker} is an entry in a Gaffer cache service which is used to store
 * details of jobs submitted to the graph.
 */
public class JobTracker extends Cache<String, JobDetail> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobTracker.class);
    private static final String CACHE_SERVICE_NAME_PREFIX = "JobTracker";
    public static final String JOB_TRACKER_CACHE_SERVICE_NAME = "JobTracker";

    /**
     * Executor to allow queuing up async operations on the cache.
     * This is largely to allow additions to the cache to not block the calling
     * thread, which would impact overall operation performance. Gets from the
     * cache are executed in the pool, but as it has a thread size of one they wait
     * until all queued up executions finish to prevent race conditions.
     */
    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());

    public JobTracker(final String suffixJobTrackerCacheName) {
        super(getCacheNameFrom(suffixJobTrackerCacheName), JOB_TRACKER_CACHE_SERVICE_NAME);
    }

    public static String getCacheNameFrom(final String suffixJobTrackerCacheName) {
        return Cache.getCacheNameFrom(CACHE_SERVICE_NAME_PREFIX, suffixJobTrackerCacheName);
    }

    public String getSuffixCacheName() {
        return getSuffixCacheNameWithoutPrefix(CACHE_SERVICE_NAME_PREFIX);
    }

    /**
     * Add or update the job details relating to a job in the job tracker cache.
     *
     * @param jobDetail the job details to update
     * @param user      the user making the request
     */
    public void addOrUpdateJob(final JobDetail jobDetail, final User user) {
        validateJobDetail(jobDetail);
        executor.submit(() -> {
            try {
                super.addToCache(jobDetail.getJobId(), jobDetail, true);
            } catch (final CacheOperationException e) {
                LOGGER.error("Failed to add jobDetail " + jobDetail.toString() + " to the cache", e);
            }
        });
    }

    /**
     * Get the details of a specific job.
     *
     * @param jobId the ID of the job to lookup
     * @param user  the user making the request to the job tracker
     * @return the {@link JobDetail} object for the requested job
     */
    public JobDetail getJob(final String jobId, final User user) {
        try {
            return executor.submit(() -> super.getFromCache(jobId)).get();
        } catch (final ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get all jobs from the job tracker cache.
     *
     * @param user the user making the request to the job tracker
     * @return a {@link Iterable} containing all of the job details
     */
    public Iterable<JobDetail> getAllJobs(final User user) {
        try {
            return executor.submit(() -> getAllJobsMatching(user, jd -> true)).get();
        } catch (final ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get all scheduled jobs from the job tracker cache.
     *
     * @return a {@link Iterable} containing all of the scheduled job details
     */
    public Iterable<JobDetail> getAllScheduledJobs() {
        try {
            return executor.submit(() ->
                getAllJobsMatching(new User(), jd -> jd.getStatus().equals(JobStatus.SCHEDULED_PARENT))).get();
        } catch (final ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Iterable<JobDetail> getAllJobsMatching(final User user, final Predicate<JobDetail> jobDetailPredicate) {
        return () -> StreamSupport.stream(getAllKeys().spliterator(), false)
            .filter(Objects::nonNull)
            .map(jobId -> getJob(jobId, user))
            .filter(Objects::nonNull)
            .filter(jobDetailPredicate)
            .iterator();
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
