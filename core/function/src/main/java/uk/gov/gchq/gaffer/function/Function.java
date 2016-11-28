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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

/**
 * A <code>Function</code> is a (potentially configurable) logical operation. Function interfaces are loosely defined
 * (intentionally) to enable functions to be invoked generically without introspection.
 * <p>
 * Functions may consume input and/or produce output records.
 *
 * @see uk.gov.gchq.gaffer.function.ConsumerFunction
 * @see uk.gov.gchq.gaffer.function.ConsumerProducerFunction
 * @see uk.gov.gchq.gaffer.function.ProducerFunction
 */
@JsonTypeInfo(use = Id.CLASS, include = As.PROPERTY, property = "class")
public interface Function {
    /**
     * Create a deep copy of this <code>Function</code>, including any configuration values. Stateful functions should
     * have their state initialised.
     *
     * @return Deep copy of this <code>Function</code>.
     */
    Function statelessClone();
}
