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

package uk.gov.gchq.gaffer.example.gettingstarted.function.transform;

import uk.gov.gchq.gaffer.function.TransformFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import uk.gov.gchq.gaffer.function.annotation.Outputs;

@Inputs({Integer.class, Integer.class})
@Outputs(Float.class)
public class MeanTransform extends TransformFunction {

    @Override
    public Object[] transform(final Object[] input) {
        return transform((Integer) input[0], (Integer) input[1]);
    }

    public Object[] transform(final Integer total, final Integer count) {
        final float mean = total / (float) count;
        return new Object[]{mean};
    }

    @Override
    public TransformFunction statelessClone() {
        return new MeanTransform();
    }
}
