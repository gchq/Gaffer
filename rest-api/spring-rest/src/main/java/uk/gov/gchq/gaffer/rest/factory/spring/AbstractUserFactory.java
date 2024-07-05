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

package uk.gov.gchq.gaffer.rest.factory.spring;

import org.springframework.http.HttpHeaders;

import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.user.User;

/**
 * The base abstract {@link UserFactory} implementation for the spring
 * rest API. Allows setting the http headers for use in authorisation.
 */
public class AbstractUserFactory implements UserFactory {

    HttpHeaders httpHeaders;

    @Override
    public User createUser() {
        throw new UnsupportedOperationException("Unimplemented method 'createUser'");
    }

    /**
     * Allow setting the {@link HttpHeaders} the user factory may use
     * to carry out authorisation.
     *
     * @param httpHeaders the headers
     */
    public void setHttpHeaders(final HttpHeaders httpHeaders) {
        this.httpHeaders = httpHeaders;
    }

}
