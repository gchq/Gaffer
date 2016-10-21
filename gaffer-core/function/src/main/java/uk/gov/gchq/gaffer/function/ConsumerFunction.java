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
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import java.lang.annotation.AnnotationFormatError;
import java.util.Arrays;

/**
 * A <code>ConsumerFunction</code> is a {@link gaffer.function.Function} that consumes input records. Implementations
 * should be annotated with the {@link gaffer.function.annotation.Inputs} annotation specifying the input record
 * structure that the function accepts.
 */
public abstract class ConsumerFunction implements Function {
    private Class<?>[] inputs;

    @Override
    public abstract ConsumerFunction statelessClone();

    /**
     * @return Input record structure accepted by this <code>ConsumerFunction</code>.
     */
    @JsonIgnore
    public Class<?>[] getInputClasses() {
        if (null == inputs) {
            processInputAnnotation();
        }

        return Arrays.copyOf(inputs, inputs.length);
    }

    /**
     * Retrieves the input record structure from the {@link gaffer.function.annotation.Inputs} annotation.
     */
    private void processInputAnnotation() {
        final Inputs annotation = getClass().getAnnotation(Inputs.class);
        if (null == annotation) {
            throw new AnnotationFormatError("All consumer function classes must have inputs defined using the 'Inputs' annotation on the class."
                    + " Class: " + getClass());
        }

        inputs = annotation.value();
    }
}
