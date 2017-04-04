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

package uk.gov.gchq.gaffer.example.films.generator;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.example.films.data.Film;
import uk.gov.gchq.gaffer.example.films.data.Person;
import uk.gov.gchq.gaffer.example.films.data.Review;
import uk.gov.gchq.gaffer.example.films.data.Viewing;

public class DataGenerator implements OneToOneElementGenerator<Object> {
    private final ViewingGenerator viewingGenerator = new ViewingGenerator();
    private final ReviewGenerator reviewGenerator = new ReviewGenerator();
    private final PersonGenerator personGenerator = new PersonGenerator();
    private final FilmGenerator filmGenerator = new FilmGenerator();

    @Override
    public Element _apply(final Object obj) {
        if (obj instanceof Viewing) {
            return viewingGenerator._apply(((Viewing) obj));
        }

        if (obj instanceof Review) {
            return reviewGenerator._apply(((Review) obj));
        }

        if (obj instanceof Person) {
            return personGenerator._apply(((Person) obj));
        }

        if (obj instanceof Film) {
            return filmGenerator._apply(((Film) obj));
        }

        throw new IllegalArgumentException("Element could not be generated from " + obj.getClass().getName()
                + " as an applicable generator could not be found.");
    }
}
