/*
 * Copyright 2018-2019 Crown Copyright
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
package uk.gov.gchq.gaffer.graph;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.util.Request;
import uk.gov.gchq.gaffer.user.User;

/**
 * A {@code GraphRequest} is a request that will be executed on a Gaffer {@link Graph}.
 * A new {@link Context} with new jobId will be created based on your {@link Context}/{@link User}.
 *
 * @param <O> the result type of the request.
 */
public class GraphRequest<O> extends Request<O> {
    public GraphRequest(final Operation operation, final User user) {
        super(operation, user);
    }

    public GraphRequest(final Operation operation, final Context context) {
        super(operation, context);
    }
}
