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
package uk.gov.gchq.gaffer.operation.impl.add;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Validatable;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.Map;
import java.util.function.Function;

/**
 * An {@code AddElementsFromKafka} operation consumes records of a kafka topic,
 * converts each record into a Gaffer {@link Element} using the provided
 * {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} then adds these
 * elements to the Graph. This operation is a blocking operation and will never stop.
 * You will need to terminate the job when you want to stop consuming data.
 *
 * @see Builder
 */
public class AddElementsFromKafka implements
        Operation,
        Validatable {
    @Required
    private String topic;

    /**
     * The id of the consumer group
     */
    @Required
    private String groupId;
    /**
     * Comma separated list of Kafka brokers
     */
    @Required
    private String[] bootstrapServers;

    @Required
    private Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> elementGenerator;

    /**
     * The parallelism of the job to be created
     */
    private Integer parallelism;

    private boolean validate = true;
    private boolean skipInvalidElements;


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

    public void setParallelism(final Integer parallelism) {
        this.parallelism = parallelism;
    }

    public Integer getParallelism() {
        return this.parallelism;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP")
    public String[] getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(final String... bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> getElementGenerator() {
        return elementGenerator;
    }

    public void setElementGenerator(final Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    @Override
    public boolean isValidate() {
        return validate;
    }

    @Override
    public void setValidate(final boolean validate) {
        this.validate = validate;
    }

    @Override
    public boolean isSkipInvalidElements() {
        return skipInvalidElements;
    }

    @Override
    public void setSkipInvalidElements(final boolean skipInvalidElements) {
        this.skipInvalidElements = skipInvalidElements;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public ValidationResult validate() {
        final ValidationResult result = Operation.super.validate();
        if (null != bootstrapServers && bootstrapServers.length < 0) {
            result.addError("At least 1 bootstrap server is required.");
        }

        return result;
    }

    @Override
    public AddElementsFromKafka shallowClone() {
        return new AddElementsFromKafka.Builder()
                .topic(topic)
                .groupId(groupId)
                .bootstrapServers(bootstrapServers)
                .generator(elementGenerator)
                .parallelism(parallelism)
                .validate(validate)
                .skipInvalidElements(skipInvalidElements)
                .options(options)
                .build();
    }

    public static class Builder extends BaseBuilder<AddElementsFromKafka, Builder>
            implements Validatable.Builder<AddElementsFromKafka, Builder> {
        public Builder() {
            super(new AddElementsFromKafka());
        }

        public Builder generator(final Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> generator) {
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

        public Builder parallelism(final Integer parallelism) {
            _getOp().setParallelism(parallelism);
            return _self();
        }
    }
}
