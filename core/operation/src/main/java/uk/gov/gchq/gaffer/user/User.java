/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.user;

import org.apache.commons.lang.StringUtils;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class User {
    public static final String UNKNOWN_USER_ID = "UNKNOWN";
    private final String userId;
    private final Set<String> dataAuths = new HashSet<>();
    private final Set<String> opAuths = new HashSet<>();

    public User() {
        this(UNKNOWN_USER_ID);
    }

    public User(final String userId) {
        this.userId = StringUtils.isEmpty(userId) ? UNKNOWN_USER_ID : userId;
    }

    public User(final String userId, final Set<String> dataAuths) {
        this(userId);
        this.dataAuths.addAll(dataAuths);
    }

    public User(final String userId, final Set<String> dataAuths, final Set<String> opAuths) {
        this(userId);
        this.dataAuths.addAll(dataAuths);
        this.opAuths.addAll(opAuths);
    }

    public String getUserId() {
        return userId;
    }

    public Set<String> getDataAuths() {
        return Collections.unmodifiableSet(dataAuths);
    }

    public Set<String> getOpAuths() {
        return Collections.unmodifiableSet(opAuths);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final User user = (User) o;
        if (!userId.equals(user.userId)) {
            return false;
        }

        return dataAuths.equals(user.dataAuths) && opAuths.equals(user.opAuths);
    }

    @Override
    public int hashCode() {
        int result = userId.hashCode();
        result = 31 * result + dataAuths.hashCode();
        result = 31 * result + opAuths.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "User{"
                + "userId='" + userId + '\''
                + ", dataAuths=" + dataAuths
                + ", opAuths=" + opAuths
                + '}';
    }

    public static class Builder {
        private String userId;
        private final Set<String> dataAuths = new HashSet<>();
        private final Set<String> opAuths = new HashSet<>();


        public Builder userId(final String userId) {
            this.userId = userId;
            return this;
        }

        public Builder dataAuth(final String dataAuth) {
            this.dataAuths.add(dataAuth);
            return this;
        }

        public Builder dataAuths(final String... dataAuths) {
            Collections.addAll(this.dataAuths, dataAuths);
            return this;
        }

        public Builder dataAuths(final Collection<String> dataAuths) {
            this.dataAuths.addAll(dataAuths);
            return this;
        }

        public Builder opAuth(final String opAuth) {
            this.opAuths.add(opAuth);
            return this;
        }

        public Builder opAuths(final String... opAuths) {
            Collections.addAll(this.opAuths, opAuths);
            return this;
        }

        public Builder opAuths(final Collection<String> opAuths) {
            this.opAuths.addAll(opAuths);
            return this;
        }

        public User build() {
            return new User(userId, dataAuths, opAuths);
        }
    }
}
