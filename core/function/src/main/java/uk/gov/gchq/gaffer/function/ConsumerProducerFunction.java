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

package uk.gov.gchq.gaffer.function;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.function.annotation.Outputs;
import java.lang.annotation.AnnotationFormatError;
import java.util.Arrays;

/**
 * A <code>ConsumerProducerFunction</code> is a {@link uk.gov.gchq.gaffer.function.Function} that consumes input records and
 * produces output records. Implementations should be annotated with the {@link uk.gov.gchq.gaffer.function.annotation.Inputs}
 * annotation specifying the input record structure that the function accepts and the
 * {@link uk.gov.gchq.gaffer.function.annotation.Outputs} annotation specifying the output record structure that the function
 * produces.
 */
public abstract class ConsumerProducerFunction extends ConsumerFunction {
    protected Class<?>[] outputs;

    @Override
    public abstract ConsumerProducerFunction statelessClone();

    /**
     * @return Output record structure accepted by this <code>ConsumerProducerFunction</code>.
     */
    @JsonIgnore
    public Class<?>[] getOutputClasses() {
        if (null == outputs) {
            processOutputAnnotation();
        }

        return Arrays.copyOf(outputs, outputs.length);
    }

    /**
     * Retrieves the input record structure from the {@link uk.gov.gchq.gaffer.function.annotation.Inputs} annotation.
     */
    private void processOutputAnnotation() {
        final Outputs annotation = getClass().getAnnotation(Outputs.class);
        if (null == annotation) {
            throw new AnnotationFormatError("All consumer producer function classes must have outputs defined using the 'Outputs' annotation on the class."
                    + " Class: " + getClass());
        }

        outputs = annotation.value();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ConsumerProducerFunction that = (ConsumerProducerFunction) o;

        return new EqualsBuilder()
                .appendSuper(super.equals(o))
                .append(inputs, that.inputs)
                .append(outputs, that.outputs)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(inputs)
                .append(outputs)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("inputs", inputs)
                .append("outputs", outputs)
                .toString();
    }
}
