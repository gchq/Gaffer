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
package uk.gov.gchq.gaffer.doc.operation;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneObjectGenerator;
import uk.gov.gchq.gaffer.doc.operation.generator.ObjectGenerator;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;

public class GenerateObjectsExample extends OperationExample {
    public static void main(final String[] args) {
        new GenerateObjectsExample().run();
    }

    public GenerateObjectsExample() {
        super(GenerateObjects.class);
    }

    @Override
    public void runExamples() {
        generateStringsFromElements();
        generateDomainObjectsFromElements();
    }

    public Iterable<? extends String> generateStringsFromElements() {
        // ---------------------------------------------------------
        final GenerateObjects<String> operation = new GenerateObjects.Builder<String>()
                .input(new Entity.Builder()
                                .group("entity")
                                .vertex(6)
                                .property("count", 1)
                                .build(),
                        new Edge.Builder()
                                .group("edge")
                                .source(5).dest(6).directed(true)
                                .property("count", 1)
                                .build())
                .generator(new ObjectGenerator())
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public Iterable<?> generateDomainObjectsFromElements() {
        // ---------------------------------------------------------
        final GenerateObjects<Object> operation = new GenerateObjects.Builder<>()
                .input(new Entity.Builder()
                                .group("entity")
                                .vertex(6)
                                .property("count", 1)
                                .build(),
                        new Edge.Builder()
                                .group("edge")
                                .source(5).dest(6).directed(true)
                                .property("count", 1)
                                .build())
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

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("a", a)
                    .append("c", c)
                    .build();
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

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("a", a)
                    .append("b", b)
                    .append("c", c)
                    .build();
        }
    }

    public static class DomainObjectGenerator implements OneToOneObjectGenerator<Object> {
        @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
        @Override
        public Object _apply(final Element element) {
            if (element instanceof Entity) {
                return new DomainObject1((int) ((Entity) element).getVertex(), (int) element.getProperty("count"));
            } else {
                final Edge edge = (Edge) element;
                return new DomainObject2((int) edge.getSource(), (int) edge.getDestination(), (int) edge.getProperty("count"));
            }
        }
    }
}
