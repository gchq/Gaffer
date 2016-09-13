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
package gaffer.example.operation;

import gaffer.data.element.Element;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.impl.Limit;
import gaffer.operation.impl.get.GetAllElements;

public class LimitExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new LimitExample().run();
    }

    public LimitExample() {
        super(Limit.class);
    }

    @Override
    public void runExamples() {
        limitElementsTo3();
    }

    public Iterable<Element> limitElementsTo3() {
        final String opJava = "new OperationChain.Builder()\n" +
                "                .first(new GetAllElements<>())\n" +
                "                .then(new Limit.Builder<Element>()\n" +
                "                        .limitResults(3)\n" +
                "                        .build())\n" +
                "                .build()";
        return runExample(new OperationChain.Builder()
                .first(new GetAllElements<>())
                .then(new Limit.Builder<Element>()
                        .limitResults(3)
                        .build())
                .build(), opJava);
    }
}
