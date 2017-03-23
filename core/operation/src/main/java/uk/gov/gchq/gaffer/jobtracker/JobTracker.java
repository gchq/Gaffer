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


import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.user.User;

public interface JobTracker {

    /**
     * Initialises the job tracker with the provided config path.
     *
     * @param configPath the path to the job tracker configuration
     */
    void initialise(final String configPath);

    /**
     * Adds or updates the given job.
     *
     * @param jobDetail the job to add or update
     * @param user      the user running the job
     */
    void addOrUpdateJob(final JobDetail jobDetail, final User user);

    /**
     * Gets the job with the given ID.
     *
     * @param jobId the job id
     * @param user  the user requesting the job details
     * @return the job details for the given job id
     */
    JobDetail getJob(String jobId, User user);

    /**
     * Gets all the job details.
     *
     * @param user the user requesting the job details
     * @return the job details for the given job id
     */
    CloseableIterable<JobDetail> getAllJobs(User user);

    /**
     * Clears the cache.
     */
    void clear();
}
