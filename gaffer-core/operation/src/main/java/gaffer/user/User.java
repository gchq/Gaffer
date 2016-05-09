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
package gaffer.user;

import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class User {
    private static final String UNKNOWN_USER_ID = "UNKNOWN";
    private String userId;
    private Set<String> dataAuths = new HashSet<>();

    public User() {
        this(UNKNOWN_USER_ID);
    }

    public User(final String userId) {
        this.userId = userId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(final String userId) {
        this.userId = userId;
    }

    public Set<String> getDataAuths() {
        return dataAuths;
    }

    public void addDataAuth(final String dataAuth) {
        dataAuths.add(dataAuth);
    }

    public void setDataAuths(final Set<String> dataAuths) {
        if (null != dataAuths) {
            this.dataAuths = dataAuths;
        } else {
            this.dataAuths = new HashSet<>();
        }
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
        return dataAuths.equals(user.dataAuths);

    }

    @Override
    public int hashCode() {
        int result = userId.hashCode();
        result = 31 * result + dataAuths.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "User{"
                + "userId='" + userId + '\''
                + ", dataAuths=" + dataAuths
                + '}';
    }

    public static class Builder {
        private User user = new User();

        public Builder userId(final String userId) {
            user.setUserId(userId);
            return this;
        }

        public Builder dataAuths(final String... dataAuths) {
            return dataAuths(Sets.newHashSet(dataAuths));
        }

        public Builder dataAuths(final Collection<String> dataAuths) {
            user.getDataAuths().addAll(dataAuths);
            return this;
        }

        public Builder dataAuth(final String dataAuth) {
            user.addDataAuth(dataAuth);
            return this;
        }

        public User build() {
            return user;
        }
    }
}
