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

package gaffer.function;

import com.fasterxml.jackson.annotation.JsonIgnore;
import gaffer.function.annotation.Outputs;
import java.lang.annotation.AnnotationFormatError;
import java.util.Arrays;

/**
 * A <code>ConsumerProducerFunction</code> is a {@link gaffer.function.Function} that consumes input records and
 * produces output records. Implementations should be annotated with the {@link gaffer.function.annotation.Inputs}
 * annotation specifying the input record structure that the function accepts and the
 * {@link gaffer.function.annotation.Outputs} annotation specifying the output record structure that the function
 * produces.
 */
public abstract class ConsumerProducerFunction extends ConsumerFunction {
    private Class<?>[] outputs;

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
     * Retrieves the input record structure from the {@link gaffer.function.annotation.Inputs} annotation.
     */
    private void processOutputAnnotation() {
        final Outputs annotation = getClass().getAnnotation(Outputs.class);
        if (null == annotation || null == annotation.value()) {
            throw new AnnotationFormatError("All consumer producer function classes must have outputs defined using the 'Outputs' annotation on the class.");
        }

        outputs = annotation.value();
    }
}
