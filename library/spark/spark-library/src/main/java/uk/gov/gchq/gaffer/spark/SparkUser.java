/*
 * Copyright 2017. Crown Copyright
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

package uk.gov.gchq.gaffer.spark;


import org.apache.spark.sql.SparkSession;
import uk.gov.gchq.gaffer.user.User;

/**
 *
 */
public class SparkUser extends User {

    private SparkSession sparkSession;

    public SparkUser(final User user, final SparkSession session) {
        super(user.getUserId(), user.getDataAuths(), user.getOpAuths());
        setSparkSession(session);
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(final SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public boolean equals(final Object user) {
        return (user instanceof SparkUser &&
                this.sparkSession.equals(((SparkUser) user).getSparkSession()) &&
                super.equals(user));
    }

    @Override
    public int hashCode() {
        return super.hashCode() + this.sparkSession.hashCode();
    }
}

