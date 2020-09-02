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

package uk.gov.gchq.gaffer.access;

import uk.gov.gchq.gaffer.user.User;

public interface AccessControlledResource {
    String DONT_CHECK_ADMIN_AUTH = null;

    ResourceType getResourceType();

    default boolean hasReadAccess(final User user) {
        return hasReadAccess(user, DONT_CHECK_ADMIN_AUTH);
    }

    boolean hasReadAccess(final User user, final String adminAuth);

    default boolean hasWriteAccess(final User user) {
        return hasWriteAccess(user, DONT_CHECK_ADMIN_AUTH);
    }

    boolean hasWriteAccess(final User user, final String adminAuth);
}
