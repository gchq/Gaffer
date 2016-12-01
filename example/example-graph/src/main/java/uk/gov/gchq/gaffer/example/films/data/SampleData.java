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

package uk.gov.gchq.gaffer.example.films.data;

import java.util.ArrayList;
import java.util.List;

public class SampleData {
    public List<Object> generate() {
        final List<Object> data = new ArrayList<>();
        data.addAll(getViewings());
        data.addAll(getReviews());
        data.addAll(getPeople());
        data.addAll(getFilms());
        return data;
    }

    private List<Viewing> getViewings() {
        final List<Viewing> filmViews = new ArrayList<>();

        filmViews.add(new Viewing("filmA", "user01", 1401000000000L));
        filmViews.add(new Viewing("filmB", "user01", 1402000000000L));

        filmViews.add(new Viewing("filmA", "user02", 1401000000000L));
        filmViews.add(new Viewing("filmC", "user02", 1405000000000L));
        filmViews.add(new Viewing("filmC", "user02", 1407000000000L));

        filmViews.add(new Viewing("filmA", "user03", 1408000000000L));
        filmViews.add(new Viewing("filmB", "user03", 1409000000000L));

        return filmViews;
    }

    private List<Review> getReviews() {
        final List<Review> reviews = new ArrayList<>();

        reviews.add(new Review("filmA", "user01", 70));
        reviews.add(new Review("filmB", "user01", 80));

        reviews.add(new Review("filmA", "user02", 65));
        reviews.add(new Review("filmC", "user02", 95));

        reviews.add(new Review("filmA", "user03", 30));

        return reviews;
    }

    private List<Person> getPeople() {
        final List<Person> people = new ArrayList<>();

        people.add(new Person("user01", "User 01", 10));
        people.add(new Person("user02", "User 02", 30));
        people.add(new Person("user03", "User 03", 50));

        return people;
    }

    private List<Film> getFilms() {
        final List<Film> films = new ArrayList<>();
        films.add(new Film("filmA", "Film A", Certificate.U));
        films.add(new Film("filmB", "Film B", Certificate.PG));
        films.add(new Film("filmC", "Film C", Certificate._15));

        return films;
    }

}
