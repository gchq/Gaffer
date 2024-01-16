/*
 * Copyright 2024 Crown Copyright
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

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.user.User;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.Properties;

public class JobTrackerBrokenCacheTest {
    @Mock
    private Logger loggerMock;

    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.jobtracker.BrokenCacheService";
    private static Properties properties = new Properties();

    @BeforeAll
    public static void setUp() {
        properties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, CACHE_SERVICE_CLASS_STRING);
        CacheServiceLoader.initialise(properties);

    }

    @Test
    public void jobTrackerDoesNotThrowExceptionWhenCacheIsBroken() {
        // Given
        JobTracker jobTracker = new JobTracker("Test");
        final OperationChain operationChain = new OperationChain.Builder().first(new GetAllElements()).build();
        final User user = new User("user");
        final JobDetail jobDetail = new JobDetail.Builder()
                .description("desc")
                .jobId("1")
                .parentJobId("2")
                .repeat(new Repeat(20L, 30L, TimeUnit.MINUTES))
                .status(JobStatus.RUNNING)
                .user(new User("user"))
                .opChain(operationChain)
                .serialisedOperationChain(operationChain)
                .build();

        assertThatCode(() -> jobTracker.addOrUpdateJob(jobDetail, new User("user"))).doesNotThrowAnyException();
    }
}
