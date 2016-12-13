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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.dataframe;

/**
 * A simple object used to test that custom properties can be converted into a type suitable for inclusion in a
 * <code>Dataframe</code> if the user provides a suitable <code>Converter</code>.
 */
public class MyProperty {
    private int a;

    MyProperty() {

    }

    MyProperty(final int a) {
        this.a = a;
    }

    int getA() {
        return a;
    }
}
