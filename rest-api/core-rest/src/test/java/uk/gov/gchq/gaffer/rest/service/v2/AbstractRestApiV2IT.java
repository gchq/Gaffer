/*
 * Copyright 2017 Crown Copyright
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

import uk.gov.gchq.gaffer.rest.AbstractRestApiIT;
import uk.gov.gchq.gaffer.rest.RestApiTestClient;

public class AbstractRestApiV2IT extends AbstractRestApiIT<RestApiV2TestClient> {

    @Override
    protected RestApiV2TestClient getClient() {
        return new RestApiV2TestClient();
    }
}
