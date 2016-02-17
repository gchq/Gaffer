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

package gaffer.example.generator;

import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.generator.OneToOneElementGenerator;
import gaffer.example.data.Film;
import gaffer.example.data.Person;
import gaffer.example.data.Review;
import gaffer.example.data.Viewing;
import gaffer.example.data.schema.Group;

public class DataGenerator extends OneToOneElementGenerator<Object> {
    private final ViewingGenerator viewingGenerator = new ViewingGenerator();
    private final ReviewGenerator reviewGenerator = new ReviewGenerator();
    private final PersonGenerator personGenerator = new PersonGenerator();
    private final FilmGenerator filmGenerator = new FilmGenerator();

    @Override
    public Element getElement(final Object obj) {
        if (obj instanceof Viewing) {
            return viewingGenerator.getElement(((Viewing) obj));
        }

        if (obj instanceof Review) {
            return reviewGenerator.getElement(((Review) obj));
        }

        if (obj instanceof Person) {
            return personGenerator.getElement(((Person) obj));
        }

        if (obj instanceof Film) {
            return filmGenerator.getElement(((Film) obj));
        }

        throw new IllegalArgumentException("Element could not be generated from " + obj.getClass().getName()
                + " as an applicable generator could not be found.");
    }

    @Override
    public Object getObject(final Element element) {
        final String group = element.getGroup();
        if (element instanceof Entity) {
            if (Group.REVIEW.equals(group)) {
                return reviewGenerator.getObject(element);
            }
            if (Group.PERSON.equals(group)) {
                return personGenerator.getObject(element);
            }
            if (Group.FILM.equals(group)) {
                return filmGenerator.getObject(element);
            }
        } else if (element instanceof Edge) {
            if (Group.VIEWING.equals(group)) {
                return viewingGenerator.getObject(element);
            }
        }

        throw new IllegalArgumentException("Domain object could not be generated from an element of type " + group
                + " as an applicable generator could not be found.");
    }
}
