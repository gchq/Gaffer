/*
 * Copyright 2016-2020 Crown Copyright
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
package uk.gov.gchq.gaffer.integration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import uk.gov.gchq.gaffer.integration.extensions.AwaitSpringStart;
import uk.gov.gchq.gaffer.integration.factory.MapStoreGraphFactory;
import uk.gov.gchq.gaffer.rest.GafferWebApplication;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.user.User;

import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.DEFINED_PORT;

/**
 * Common code for integration tests. This class is annotated with @SpringBootTest to start a Gaffer REST API so the
 * ProxyStore can be tested. To save startup time, the Application context will be cached between tests.
 */
@SpringBootTest(classes = GafferWebApplication.class, webEnvironment = DEFINED_PORT)
@ExtendWith(AwaitSpringStart.class)
@ActiveProfiles("proxy")
public abstract class AbstractStoreIT {

    @BeforeAll
    public static void beforeAll() {
        System.out.println("hello");
    }

    @Autowired
    private GraphFactory graphFactory;

    @BeforeEach
    public void resetRemoteProxyGraph() {
        if (graphFactory instanceof MapStoreGraphFactory) {
            ((MapStoreGraphFactory) graphFactory).reset();
        } else {
            throw new RuntimeException("Expected the MapStoreGraph Factory to be injected");
        }
    }

    private User user = new User();

    public User getUser() {
        return user;
    }
}
