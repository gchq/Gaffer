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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.ICacheService;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.user.User;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doThrow;
import static uk.gov.gchq.gaffer.jobtracker.JobTracker.JOB_TRACKER_CACHE_SERVICE_NAME;

public class JobTrackerBrokenCacheTest {
    @BeforeAll
    public static void setUp() throws Exception {
        ICache<Object, Object> mockICache = Mockito.mock(ICache.class);
        doThrow(new CacheOperationException("Stubbed class")).when(mockICache).put(any(), any());
        ICacheService mockICacheService = Mockito.spy(ICacheService.class);
        given(mockICacheService.getCache(any())).willReturn(mockICache);

        Field field = CacheServiceLoader.class.getDeclaredField("SERVICES");
        field.setAccessible(true);
        Map<String, ICacheService> mockCacheServices = (Map<String, ICacheService>) field.get(new HashMap<>());
        mockCacheServices.put(JOB_TRACKER_CACHE_SERVICE_NAME, mockICacheService);
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
                .user(user)
                .opChain(operationChain)
                .serialisedOperationChain(operationChain)
                .build();

        assertThatCode(() -> jobTracker.addOrUpdateJob(jobDetail, user)).doesNotThrowAnyException();
    }
}
