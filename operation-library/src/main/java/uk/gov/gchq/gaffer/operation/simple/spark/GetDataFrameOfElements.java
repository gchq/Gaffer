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
package uk.gov.gchq.gaffer.operation.simple.spark;

import uk.gov.gchq.gaffer.operation.AbstractGetOperation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Collections;
import java.util.LinkedHashSet;

/**
 * An <code>Operation</code> that returns an Apache Spark <code>DataFrame</code> (i.e. a {@link Dataset} of
 * {@link Row}s) consisting of the <code>Element</code>s converted to {@link Row}s. It allows a set of groups to
 * be specified in order. Elements of these groups will be present in the <code>DataFrame</code>.
 * <p>
 * Implementations of this operation should automatically convert all properties that have natural equivalents
 * as a Spark <code>DataType</code> to that <code>DataType</code>. An implementation may allow the user to
 * specify a conversion function for properties that do not have natural equivalents. Thus not all properties
 * from each <code>Element</code> will necessarily make it into the <code>DataFrame</code>.
 * <p>
 * The schema of the <code>Dataframe</code> is formed of all properties from the first group, followed by all
 * properties from the second group, with the exception of properties already found in the first group, etc.
 */
public class GetDataFrameOfElements extends AbstractGetOperation<Void, Dataset<Row>> {

    private SQLContext sqlContext;
    private LinkedHashSet<String> groups;

    public GetDataFrameOfElements() { }

    public GetDataFrameOfElements(final SQLContext sqlContext,
                                  final String group) {
        this.sqlContext = sqlContext;
        this.groups = new LinkedHashSet<>(Collections.singleton(group));
    }

    public GetDataFrameOfElements(final SQLContext sqlContext,
                                  final LinkedHashSet<String> groups) {
        this.sqlContext = sqlContext;
        this.groups = new LinkedHashSet<>(groups);
    }

    public void setSqlContext(final SQLContext sqlContext) {
        this.sqlContext = sqlContext;
    }

    public SQLContext getSqlContext() {
        return sqlContext;
    }

    public void setGroups(final LinkedHashSet<String> groups) {
        this.groups = groups;
    }

    public LinkedHashSet<String> getGroups() {
        return groups;
    }

    public abstract static class BaseBuilder <CHILD_CLASS extends BaseBuilder<?>>
            extends AbstractGetOperation.BaseBuilder<GetDataFrameOfElements, Void, Dataset<Row>, CHILD_CLASS> {

        public BaseBuilder() {
            this(new GetDataFrameOfElements());
        }

        public BaseBuilder(final GetDataFrameOfElements op) {
            super(op);
        }

        public CHILD_CLASS sqlContext(final SQLContext sqlContext) {
            op.setSqlContext(sqlContext);
            return self();
        }

        public CHILD_CLASS group(final String group) {
            op.setGroups(new LinkedHashSet<>(Collections.singleton(group)));
            return self();
        }

        public CHILD_CLASS groups(final LinkedHashSet<String> groups) {
            op.setGroups(groups);
            return self();
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        public Builder() {
        }

        public Builder(final GetDataFrameOfElements op) {
            super(op);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
