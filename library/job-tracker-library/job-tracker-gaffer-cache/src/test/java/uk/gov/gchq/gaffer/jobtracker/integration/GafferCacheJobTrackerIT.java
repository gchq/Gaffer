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

package uk.gov.gchq.gaffer.jobtracker.integration;


import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.cache.util.CacheSystemProperty;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.hazelcast.cache.HazelcastCacheService;
import uk.gov.gchq.gaffer.jcs.cache.JcsCacheService;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jobtracker.JobStatus;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEdges;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class GafferCacheJobTrackerIT {

    private static Graph graph;
    private static Store store;

    @BeforeClass
    public static void setUp() throws ClassNotFoundException, StoreException, IllegalAccessException, InstantiationException {
        System.setProperty(CacheSystemProperty.CACHE_SERVICE_CLASS, HashMapCacheService.class.getCanonicalName());
        CacheServiceLoader.initialise();

        final StoreProperties storeProps = StoreProperties.loadStoreProperties(StreamUtil.storeProps(GafferCacheJobTrackerIT.class));
        store = Class.forName(storeProps.getStoreClass()).asSubclass(Store.class).newInstance();
        store.initialise(new Schema(), storeProps);
        graph = new Graph.Builder()
                .store(store)
                .build();
    }

    private void reInitialiseCacheService(Class clazz, File configFile) {
        System.setProperty(CacheSystemProperty.CACHE_SERVICE_CLASS, clazz.getCanonicalName());
        if (configFile != null) {
            System.setProperty(CacheSystemProperty.CACHE_CONFIG_FILE, configFile.getAbsolutePath());
        }
        CacheServiceLoader.initialise();
    }

    @Before
    public void beforeEach() {
        System.clearProperty(CacheSystemProperty.CACHE_CONFIG_FILE);
        System.clearProperty(CacheSystemProperty.CACHE_SERVICE_CLASS);
        if (store != null) {
            store.getJobTracker().clear();
        }
    }

    @Test
    public void shouldWorkWhenUsingHashMapCache() throws OperationException, InterruptedException, IOException {
        reInitialiseCacheService(HashMapCacheService.class, null);
        runTests();
    }

    @Test
    public void shouldWorkWhenUsingHazelcastCacheWithNoConfigSet() throws OperationException, InterruptedException, IOException {
        reInitialiseCacheService(HazelcastCacheService.class, null);
        runTests();
    }

    @Test
    public void shouldRunWhenUsingConfiguredHazelcast() throws OperationException, InterruptedException, IOException {
        File configFile = new File("src/test/resources/cache/hazelcast.xml");
        reInitialiseCacheService(HazelcastCacheService.class, configFile);

        runTests();
    }

    @Test
    public void shouldWorkWhenUsingJcsWithNoConfigIsSet() throws OperationException, InterruptedException, IOException {
        reInitialiseCacheService(JcsCacheService.class, null);
        runTests();
    }

    @Test
    public void shouldWorkWhenUsingConfiguredJcs() throws OperationException, InterruptedException, IOException {
        File configFile = new File("src/test/resources/cache/cache.ccf");
        reInitialiseCacheService(JcsCacheService.class, configFile);

        runTests();
    }


    private void runTests() throws OperationException, InterruptedException, IOException {
        shouldAddJobIdToJobTrackerWhenExecuteJob();
        shouldAddJobIdToJobTrackerWhenExecute();
    }


    private void shouldAddJobIdToJobTrackerWhenExecuteJob() throws OperationException, IOException, InterruptedException {


        // Given
        final OperationChain<CloseableIterable<Edge>> opChain = new OperationChain<>(new GetAllEdges());
        final User user = new User("user01");

        // When
        JobDetail jobDetails = graph.executeJob(opChain, user);
        final String jobId = jobDetails.getJobId();
        final JobDetail expectedJobDetail = new JobDetail(jobId, user.getUserId(), opChain, JobStatus.FINISHED, null);
        expectedJobDetail.setStartTime(jobDetails.getStartTime());

        int count = 0;
        while (JobStatus.RUNNING == jobDetails.getStatus() && ++count < 20) {
            Thread.sleep(100);
            jobDetails = graph.execute(new GetJobDetails.Builder()
                    .jobId(jobId)
                    .build(), user);
        }
        expectedJobDetail.setEndTime(jobDetails.getEndTime());

        // Then
        assertEquals(expectedJobDetail, jobDetails);
    }


    private void shouldAddJobIdToJobTrackerWhenExecute() throws OperationException, IOException, InterruptedException {
        // Given
        final OperationChain<JobDetail> opChain = new OperationChain.Builder()
                .first(new GetAllEdges())
                .then(new GetJobDetails())
                .build();
        final User user = new User("user01");

        // When
        final JobDetail jobDetails = graph.execute(opChain, user);
        final String jobId = jobDetails.getJobId();
        final JobDetail expectedJobDetail = new JobDetail(jobId, user.getUserId(), opChain, JobStatus.RUNNING, null);
        expectedJobDetail.setStartTime(jobDetails.getStartTime());
        expectedJobDetail.setEndTime(jobDetails.getEndTime());

        // Then
        assertEquals(expectedJobDetail, jobDetails);
    }

}