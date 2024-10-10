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

package uk.gov.gchq.gaffer.federated.simple.access;

import uk.gov.gchq.gaffer.access.AccessControlledResource;
import uk.gov.gchq.gaffer.access.ResourceType;
import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.UnrestrictedAccessPredicate;
import uk.gov.gchq.gaffer.user.User;

import static uk.gov.gchq.gaffer.federated.simple.FederatedStore.FEDERATED_STORE_SYSTEM_USER;

/**
 * Access control for a Graph that as been added through a federated store.
 */
public class GraphAccess implements AccessControlledResource  {
    // Default accesses applied to a graph can be overridden using builder
    private boolean isPublic = true;
    private String owner = User.UNKNOWN_USER_ID;
    private AccessPredicate readAccessPredicate = new UnrestrictedAccessPredicate();
    private AccessPredicate writeAccessPredicate = new UnrestrictedAccessPredicate();

    public boolean isPublic() {
        return isPublic;
    }

    public String getOwner() {
        return owner;
    }

    public AccessPredicate getReadAccessPredicate() {
        return readAccessPredicate;
    }

    public AccessPredicate getWriteAccessPredicate() {
        return writeAccessPredicate;
    }

    @Override
    public ResourceType getResourceType() {
        return ResourceType.FederatedStoreGraph;
    }

    @Override
    public boolean hasReadAccess(final User user, final String adminAuth) {
        return isPublic()
            || user.getUserId().equals(FEDERATED_STORE_SYSTEM_USER)
            || readAccessPredicate.test(user, adminAuth);
    }

    @Override
    public boolean hasWriteAccess(final User user, final String adminAuth) {
        return user.getUserId().equals(FEDERATED_STORE_SYSTEM_USER)
            || writeAccessPredicate.test(user, adminAuth);
    }

    public static class Builder {
        GraphAccess graphAccess = new GraphAccess();

        public GraphAccess build() {
            return graphAccess;
        }

        public Builder isPublic(final boolean isPublic) {
            graphAccess.isPublic = isPublic;
            return this;
        }

        public Builder owner(final String owner) {
            graphAccess.owner = owner;
            return this;
        }

        public Builder readAccessPredicate(final AccessPredicate readAccessPredicate) {
            graphAccess.readAccessPredicate = readAccessPredicate;
            return this;
        }

        public Builder writeAccessPredicate(final AccessPredicate writeAccessPredicate) {
            graphAccess.writeAccessPredicate = writeAccessPredicate;
            return this;
        }
    }

}
