/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.flink.operation;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Options;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;

public class AddElementsFromKafka implements
        Operation,
        Options {
    private String topic;
    /**
     * The id of the consumer group
     */
    private String groupId;
    /**
     * Comma separated list of Kafka brokers
     */
    private String[] bootstrapServers;
    /**
     * The name of the job to be created
     */
    private String jobName;
    /**
     * The parallelism of the job to be created
     */
    private int parallelism;

    private Function<Iterable<? extends String>, Iterable<? extends Element>> elementGenerator;
    private Map<String, String> options;

    public String getTopic() {
        return topic;
    }

    public void setTopic(final String topic) {
        this.topic = topic;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(final String groupId) {
        this.groupId = groupId;
    }

    public String getJobName() {
        return this.jobName;
    }

    public void setJobName(final String jobName) {
        this.jobName = jobName;
    }

    public void setParallelism(final int parallelism) {
        this.parallelism = parallelism;
    }

    public int getParallelism() {
        return this.parallelism;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP")
    public String[] getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(final String... bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public <T extends Function<Iterable<? extends String>, Iterable<? extends Element>> & Serializable> T getElementGenerator() {
        return (T) elementGenerator;
    }

    public <T extends Function<Iterable<? extends String>, Iterable<? extends Element>> & Serializable> void setElementGenerator(final T elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public static class Builder extends BaseBuilder<AddElementsFromKafka, Builder>
            implements Options.Builder<AddElementsFromKafka, Builder> {
        public Builder() {
            super(new AddElementsFromKafka());
        }

        public <T extends Function<Iterable<? extends String>, Iterable<? extends Element>> & Serializable> Builder generator(final T generator) {
            _getOp().setElementGenerator(generator);
            return _self();
        }

        public Builder topic(final String topic) {
            _getOp().setTopic(topic);
            return _self();
        }

        public Builder groupId(final String groupId) {
            _getOp().setGroupId(groupId);
            return _self();
        }

        public Builder bootstrapServers(final String... bootstrapServers) {
            _getOp().setBootstrapServers(bootstrapServers);
            return _self();
        }

        public Builder jobName(final String jobName) {
            _getOp().setJobName(jobName);
            return _self();
        }

        public Builder parallelism(final int parallelism) {
            _getOp().setParallelism(parallelism);
            return _self();
        }

        public Builder options(final Map<String, String> options) {
            _getOp().setOptions(options);
            return _self();
        }
    }
}
