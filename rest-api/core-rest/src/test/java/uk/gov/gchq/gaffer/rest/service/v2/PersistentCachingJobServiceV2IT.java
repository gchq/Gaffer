/*
 * Copyright 2020 Crown Copyright
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
package uk.gov.gchq.gaffer.rest.service.v2;

import org.apache.commons.jcs.engine.control.CompositeCacheManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.jobtracker.Job;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jobtracker.JobStatus;
import uk.gov.gchq.gaffer.jobtracker.Repeat;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.job.GetAllJobDetails;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PersistentCachingJobServiceV2IT extends AbstractRestApiV2IT {

    private static final Log LOG = LogFactory.getLog(PersistentCachingJobServiceV2IT.class);
    private static final String STORE_PROPERTIES_RESOURCE_PATH = "/persistent-caching-store.properties";

    public PersistentCachingJobServiceV2IT() {
        super(StreamUtil.SCHEMA, STORE_PROPERTIES_RESOURCE_PATH);
    }

    @BeforeEach
    public void before() throws IOException {

        clearDiskCacheFiles();
        super.before();
    }

    private void clearDiskCacheFiles() throws IOException {

        final Path diskCachePath = Paths.get("target/indexed-disk-cache");

        if (Files.exists(diskCachePath)) {

            Files.walk(diskCachePath)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    public void shouldKeepScheduledJobsRunningAfterRestart() throws IOException {
        final long initialDelay = 1;
        final long repeatPeriod = 1;
        final long sleepPeriod = 5;

        // Given - schedule jobs
        final String parentJobId1 = scheduleJob(new Repeat(initialDelay, repeatPeriod, SECONDS)).getJobId();
        final String parentJobId2 = scheduleJob(new Repeat(initialDelay, repeatPeriod, SECONDS)).getJobId();

        sleepFor(sleepPeriod, SECONDS);

        // When - get all JobDetails
        final List<JobDetail> allJobDetails = getAllJobDetails();

        display(allJobDetails);

        // Then - assert job has status of SCHEDULED_PARENT
        assertEquals(JobStatus.SCHEDULED_PARENT,
                allJobDetails.stream().filter(jobDetail -> jobDetail.getJobId().equals(parentJobId1)).findFirst().get().getStatus());
        assertEquals(JobStatus.SCHEDULED_PARENT,
                allJobDetails.stream().filter(jobDetail -> jobDetail.getJobId().equals(parentJobId2)).findFirst().get().getStatus());

        // Restart server to check Jobs still scheduled
        client.stopServer();
        // Force re-initialisation of the JCS CompositeCacheManager singleton
        resetCompositeCacheManagerState();
        client.reinitialiseGraph();

        final List<JobDetail> allJobDetailsAfterRestart = getAllJobDetails();

        LOG.debug("All JobDetail following restart");
        display(allJobDetailsAfterRestart);

        final long job1InitialRunCountAfterRestart = allJobDetailsAfterRestart.stream().filter(jobDetail -> parentJobId1.equals(jobDetail.getParentJobId())).count();
        final long job2InitialRunCountAfterRestart = allJobDetailsAfterRestart.stream().filter(jobDetail -> parentJobId2.equals(jobDetail.getParentJobId())).count();

        sleepFor(sleepPeriod, SECONDS);

        final List<JobDetail> allJobDetailsAfterSleep = getAllJobDetails();

        if (LOG.isDebugEnabled()) {
            LOG.debug(format("All JobDetail after sleep for %s %s", sleepPeriod, SECONDS.toString()));
        }
        display(allJobDetailsAfterSleep);

        final long job1RunCountAfterSleep = allJobDetailsAfterSleep.stream().filter(jobDetail -> parentJobId1.equals(jobDetail.getParentJobId())).count();
        final long job2RunCountAfterSleep = allJobDetailsAfterSleep.stream().filter(jobDetail -> parentJobId2.equals(jobDetail.getParentJobId())).count();

        // Then - assert parent is of Scheduled parent still
        assertEquals(JobStatus.SCHEDULED_PARENT,
                getAllJobDetails().stream().filter(jobDetail -> jobDetail.getJobId().equals(parentJobId1)).findFirst().get().getStatus());
        assertEquals(JobStatus.SCHEDULED_PARENT,
                getAllJobDetails().stream().filter(jobDetail -> jobDetail.getJobId().equals(parentJobId2)).findFirst().get().getStatus());

        final long expectedAdditionalRunCount = (sleepPeriod / repeatPeriod);
        final long job1ExpectedFinalRunCount = job1InitialRunCountAfterRestart + expectedAdditionalRunCount;
        final long job2ExpectedFinalRunCount = job2InitialRunCountAfterRestart + expectedAdditionalRunCount;

        assertTrue(job1RunCountAfterSleep == job1ExpectedFinalRunCount);
        assertTrue(job2RunCountAfterSleep == job2ExpectedFinalRunCount);
    }

    private JobDetail scheduleJob(final Repeat repeat) throws IOException {

        final Job job = new Job(repeat, new OperationChain.Builder().first(new GetAllElements()).build());
        final Response scheduleResponse = client.scheduleJob(job);
        return scheduleResponse.readEntity(new GenericType<JobDetail>() {
        });
    }

    private List<JobDetail> getAllJobDetails() throws IOException {

        final Response allJobDetailsResponse2 = client.executeOperation(new GetAllJobDetails());
        return allJobDetailsResponse2.readEntity(new GenericType<List<JobDetail>>() {
        });
    }

    private void sleepFor(final long duration, final TimeUnit timeUnit) {

        try {
            Thread.sleep(timeUnit.toMillis(duration));
        } catch (InterruptedException exception) {
            /* intentional */
        }
    }

    /* Using reflection to reset singleton CompositeCacheManager state */
    private void resetCompositeCacheManagerState() {

        try {

            final Field instance = CompositeCacheManager.class.getDeclaredField("instance");
            instance.setAccessible(true);
            instance.set(null, null);

        } catch (NoSuchFieldException | IllegalAccessException exception) {

            throw new RuntimeException(exception);
        }
    }

    private <T> void display(final List<T> items) {
        if (LOG.isDebugEnabled()) {
            items.forEach(LOG::debug);
        }
    }
}
