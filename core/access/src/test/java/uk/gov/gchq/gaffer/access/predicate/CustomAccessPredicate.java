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

package uk.gov.gchq.gaffer.access.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import uk.gov.gchq.gaffer.user.User;

import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class CustomAccessPredicate extends AccessPredicate {

    private final List<String> auths;
    private final String userId;
    private final Map<String, String> map;

    public CustomAccessPredicate() {
        this("userId");
    }

    public CustomAccessPredicate(final String userId) {
        this(userId, emptyMap(), emptyList());
    }

    @JsonCreator
    public CustomAccessPredicate(
            @JsonProperty("userId") final String userId,
            @JsonProperty("map") final Map<String, String> map,
            @JsonProperty("auths") final List<String> auths) {
        this.userId = userId;
        this.map = map;
        this.auths = auths;
    }

    @Override
    public boolean test(final User user, final String s) {
        return false;
    }

    public String getUserId() {
        return userId;
    }

    public Map<String, String> getMap() {
        return map;
    }

    @Override
    public List<String> getAuths() {
        return auths;
    }
}
