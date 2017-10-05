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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.spark.sql.SparkSession;

import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.user.User;

/**
 * Implementation of the {@link User} class for creating users which can access
 * a {@link SparkSession}.
 */
public class SparkContext extends Context {

    public SparkContext(final SparkSession sparkSession) {
        this(new User(), sparkSession);
    }

    public SparkContext(final User user, final SparkSession sparkSession) {
        this(user, createJobId(), sparkSession);
    }

    public SparkContext(final User user, final String jobId, final SparkSession sparkSession) {
        super(user, jobId);
        setSparkSession(sparkSession);
    }

    private SparkSession sparkSession;

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(final SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public boolean equals(final Object context) {
        if (this == context) {
            return true;
        }

        if (context == null || getClass() != context.getClass()) {
            return false;
        }

        final SparkContext sparkUser = (SparkContext) context;

        return new EqualsBuilder()
                .appendSuper(super.equals(context))
                .append(sparkSession, sparkUser.getSparkSession())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(sparkSession)
                .toHashCode();
    }
}

