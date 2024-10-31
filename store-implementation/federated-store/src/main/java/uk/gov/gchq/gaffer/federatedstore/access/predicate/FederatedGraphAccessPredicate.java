/*
 * Copyright 2020-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.access.predicate;

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.user.User;

import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.FEDERATED_STORE_SYSTEM_USER;

/**
 * @deprecated Marked for removal in 2.4.0 please use standard AccessPredicates
 *             going forward.
 */
@Deprecated
public abstract class FederatedGraphAccessPredicate extends AccessPredicate {

    public FederatedGraphAccessPredicate(final Predicate<User> userPredicate) {
        super(userPredicate);
    }

    @Override
    protected boolean isAdministrator(final User user, final String adminAuth) {
        return (!isNull(user)
                && (isNotEmpty(adminAuth) || user.getUserId().equals(FEDERATED_STORE_SYSTEM_USER))
                && Stream.of(adminAuth.split(Pattern.quote(","))).anyMatch(user.getOpAuths()::contains));
    }
}
