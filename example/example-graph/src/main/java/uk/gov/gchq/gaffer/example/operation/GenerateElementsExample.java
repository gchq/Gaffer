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
package uk.gov.gchq.gaffer.example.operation;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.example.operation.generator.DataGenerator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import java.util.Arrays;

public class GenerateElementsExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new GenerateElementsExample().run();
    }

    public GenerateElementsExample() {
        super(GenerateElements.class);
    }

    @Override
    public void runExamples() {
        generateElementsFromStrings();
        generateElementsFromDomainObjects();
    }

    public CloseableIterable<Element> generateElementsFromStrings() {
        // ---------------------------------------------------------
        final GenerateElements<String> operation = new GenerateElements.Builder<String>()
                .objects(Arrays.asList("1,1", "1,2,1"))
                .generator(new DataGenerator())
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public CloseableIterable<Element> generateElementsFromDomainObjects() {
        // ---------------------------------------------------------
        final GenerateElements<Object> operation = new GenerateElements.Builder<>()
                .objects(Arrays.asList(
                        new DomainObject1(1, 1),
                        new DomainObject2(1, 2, 1)))
                .generator(new DomainObjectGenerator())
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public static class DomainObject1 {
        private int a;
        private int c;

        public DomainObject1() {
        }

        public DomainObject1(final int a, final int c) {
            this.a = a;
            this.c = c;
        }

        public int getA() {
            return a;
        }

        public void setA(final int a) {
            this.a = a;
        }

        public int getC() {
            return c;
        }

        public void setC(final int c) {
            this.c = c;
        }
    }

    public static class DomainObject2 {
        private int a;
        private int b;
        private int c;

        public DomainObject2() {
        }

        public DomainObject2(final int a, final int b, final int c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        public int getA() {
            return a;
        }

        public void setA(final int a) {
            this.a = a;
        }

        public int getB() {
            return b;
        }

        public void setB(final int b) {
            this.b = b;
        }

        public int getC() {
            return c;
        }

        public void setC(final int c) {
            this.c = c;
        }
    }

    public static class DomainObjectGenerator extends OneToOneElementGenerator<Object> {
        @Override
        public Element getElement(final Object domainObject) {
            if (domainObject instanceof DomainObject1) {
                final DomainObject1 obj1 = (DomainObject1) domainObject;
                return new Entity.Builder()
                        .group("entity")
                        .vertex(obj1.a)
                        .property("count", obj1.c)
                        .build();
            } else if (domainObject instanceof DomainObject2) {
                final DomainObject2 obj1 = (DomainObject2) domainObject;
                return new Edge.Builder()
                        .group("edge")
                        .source(obj1.a)
                        .dest(obj1.b)
                        .directed(true)
                        .property("count", obj1.c)
                        .build();
            } else {
                throw new IllegalArgumentException("Unsupported domain object");
            }
        }

        @Override
        public Object getObject(final Element element) {
            throw new UnsupportedOperationException("Getting objects is not supported");
        }
    }
}
