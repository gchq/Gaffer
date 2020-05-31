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
package uk.gov.gchq.gaffer.hbasestore.operation.hdfs.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.mapreduce.VisibilityExpressionResolver;

import uk.gov.gchq.gaffer.hdfs.operation.mapper.AbstractGafferMapperTest;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.GafferMapper;

import java.util.List;

import static org.apache.hadoop.hbase.mapreduce.CellCreator.VISIBILITY_EXP_RESOLVER_CLASS;

public class AddElementsFromHdfsMapperTest extends AbstractGafferMapperTest {
    @Override
    protected void applyMapperSpecificConfiguration(final Configuration configuration) {
        configuration.set(VISIBILITY_EXP_RESOLVER_CLASS, TestVisibilityExpressionResolver.class.getName());
    }

    @Override
    protected GafferMapper getGafferMapper() {
        return new AddElementsFromHdfsMapper();
    }

    private static class TestVisibilityExpressionResolver implements VisibilityExpressionResolver {
        @Override
        public void init() {
        }

        @Override
        public List<Tag> createVisibilityExpTags(final String visExpression) {
            return null;
        }

        @Override
        public void setConf(final Configuration conf) {
        }

        @Override
        public Configuration getConf() {
            return null;
        }
    }
}
