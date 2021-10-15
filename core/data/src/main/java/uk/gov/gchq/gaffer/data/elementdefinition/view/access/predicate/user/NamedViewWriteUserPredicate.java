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
package uk.gov.gchq.gaffer.data.elementdefinition.view.access.predicate.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import uk.gov.gchq.gaffer.access.predicate.user.DefaultUserPredicate;
import uk.gov.gchq.gaffer.user.User;

import java.util.List;

import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public class NamedViewWriteUserPredicate extends DefaultUserPredicate {

    @JsonCreator
    public NamedViewWriteUserPredicate(
            @JsonProperty("creatingUserId") final String creatingUserId,
            @JsonProperty("auths") final List<String> auths) {
        super(creatingUserId, auths);
    }

    @Override
    public boolean isResourceCreator(final User user) {
        return (!isNull(user)
                && isNotEmpty(user.getUserId())
                && (isNull(this.getCreatingUserId()) || this.getCreatingUserId().equals(user.getUserId())));
    }
}
