/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.service.v1;

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.core.exception.Status;
import uk.gov.gchq.gaffer.rest.SystemStatus;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;

import javax.inject.Inject;

/**
 * An implementation of {@link IStatusService}.
 * By default it will use a singleton {@link uk.gov.gchq.gaffer.graph.Graph} generated
 * using the {@link uk.gov.gchq.gaffer.rest.factory.GraphFactory}.
 * All operations are simply delegated to the graph.
 * Pre and post operation hooks are available by extending this class and implementing
 * preOperationHook and/or postOperationHook.
 * <p>
 * By default queries will be executed with an UNKNOWN user containing no auths.
 * The {@link UserFactory#createUser()} method should be overridden and a {@link uk.gov.gchq.gaffer.user.User}
 * object should be created from the http request.
 * </p>
 */
public class StatusService implements IStatusService {

    @Inject
    private GraphFactory graphFactory;

    @Inject
    private UserFactory userFactory;

    @Override
    public SystemStatus status() {
        try {
            if (null != graphFactory.getGraph()) {
                return SystemStatus.UP;
            }
        } catch (final Exception e) {
            throw new GafferRuntimeException("Unable to create graph.", e, Status.INTERNAL_SERVER_ERROR);
        }

        return SystemStatus.DOWN;
    }
}
