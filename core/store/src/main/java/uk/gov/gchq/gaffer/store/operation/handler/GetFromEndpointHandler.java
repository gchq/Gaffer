/*
 * Copyright 2019 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler;

import org.apache.commons.io.IOUtils;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetFromEndpoint;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.io.IOException;
import java.net.URL;

public class GetFromEndpointHandler implements OutputOperationHandler<GetFromEndpoint, String> {
    @Override
    public String doOperation(final GetFromEndpoint operation, final Context context, final Store store) throws OperationException {
        try {
            return IOUtils.toString(new URL(operation.getEndpoint()));
        } catch (final IOException e) {
            throw new OperationException("Exception reading data from endpoint", e);
        }
    }
}
