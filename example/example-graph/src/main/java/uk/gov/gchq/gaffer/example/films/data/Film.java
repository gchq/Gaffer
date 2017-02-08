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

public class Film {
    private String filmId;
    private String name;
    private Certificate certificate;

    public Film() {
    }

    public Film(final String filmId, final String name, final Certificate certificate) {
        this.filmId = filmId;
        this.name = name;
        this.certificate = certificate;
    }

    public String getFilmId() {
        return filmId;
    }

    public void setFilmId(final String filmId) {
        this.filmId = filmId;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public Certificate getCertificate() {
        return certificate;
    }

    public void setCertificate(final Certificate certificate) {
        this.certificate = certificate;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof Film)) {
            return false;
        }

        final Film film = (Film) o;
        return certificate == film.certificate && filmId.equals(film.filmId) && name.equals(film.name);
    }

    @Override
    public int hashCode() {
        int result = filmId.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + certificate.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Film{"
                + "filmId='" + filmId
                + "\', name='" + name
                + "\', certificate=" + certificate
                + '}';
    }
}
