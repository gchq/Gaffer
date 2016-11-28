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
package uk.gov.gchq.gaffer.example.function.filter;

import uk.gov.gchq.gaffer.example.util.Example;
import uk.gov.gchq.gaffer.example.util.JavaSourceUtil;
import uk.gov.gchq.gaffer.function.FilterFunction;
import uk.gov.gchq.gaffer.function.SimpleFilterFunction;

public abstract class FilterFunctionExample extends Example {
    public FilterFunctionExample(final Class<? extends FilterFunction> classForExample) {
        super(classForExample);
    }

    protected void runExample(final FilterFunction filterFunction, final Object[]... inputs) {
        _runExample(filterFunction, inputs);
    }

    protected void runExample(final SimpleFilterFunction filterFunction, final Object... inputs) {
        final Object[][] wrappedInputs = new Object[inputs.length][];
        for (int i = 0; i < inputs.length; i++) {
            wrappedInputs[i] = new Object[]{inputs[i]};
        }

        _runExample(filterFunction, wrappedInputs);
    }

    private void _runExample(final FilterFunction filterFunction, final Object[][] inputs) {
        log("#### " + getMethodNameAsSentence(2) + "\n");
        printJava(JavaSourceUtil.getRawJavaSnippet(getClass(), "example/example-graph", " " + getMethodName(2) + "() {", String.format("---%n"), "// ----"));
        printAsJson(filterFunction);

        log("Input type:");
        log("\n```");
        final StringBuilder inputClasses = new StringBuilder();
        for (final Class<?> item : filterFunction.getInputClasses()) {
            inputClasses.append(item.getName());
            inputClasses.append(", ");
        }
        log(inputClasses.substring(0, inputClasses.length() - 2));
        log("```\n");

        log("Example inputs:");
        log("<table>");
        log("<tr><th>Type</th><th>Input</th><th>Result</th></tr>");
        for (final Object[] input : inputs) {
            final String inputType;
            final String inputString;
            if (1 == input.length) {
                if (null == input[0]) {
                    inputType = "";
                    inputString = "null";
                } else {
                    inputType = input[0].getClass().getName();
                    inputString = String.valueOf(input[0]);
                }
            } else {
                final StringBuilder inputTypeBuilder = new StringBuilder("[");
                final StringBuilder inputStringBuilder = new StringBuilder("[");
                for (final Object item : input) {
                    if (null == item) {
                        inputTypeBuilder.append(" ,");
                        inputStringBuilder.append("null, ");
                    } else {
                        inputTypeBuilder.append(item.getClass().getName());
                        inputTypeBuilder.append(", ");

                        inputStringBuilder.append(String.valueOf(item));
                        inputStringBuilder.append(", ");
                    }
                }
                inputType = inputTypeBuilder.substring(0, inputTypeBuilder.length() - 2) + "]";
                inputString = inputStringBuilder.substring(0, inputStringBuilder.length() - 2) + "]";
            }

            String result;
            try {
                result = String.valueOf(filterFunction.isValid(input));
            } catch (final Exception e) {
                result = e.toString();
            }

            log("<tr><td>" + inputType + "</td><td>" + inputString + "</td><td>" + result + "</td></tr>");
        }
        log("</table>\n");
        log(METHOD_DIVIDER);
    }
}
