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

public class Review {
    private String filmId;
    private String userId;
    private int rating;

    public Review() {
    }

    public Review(final String filmId, final String userId, final int rating) {
        this.filmId = filmId;
        this.userId = userId;
        this.rating = rating;
    }

    public String getFilmId() {
        return filmId;
    }

    public void setFilmId(final String filmId) {
        this.filmId = filmId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(final String userId) {
        this.userId = userId;
    }

    public int getRating() {
        return rating;
    }

    public void setRating(final int rating) {
        this.rating = rating;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof Review)) {
            return false;
        }

        final Review review = (Review) o;
        return rating == review.rating && filmId.equals(review.filmId) && userId.equals(review.userId);

    }

    @Override
    public int hashCode() {
        int result = filmId.hashCode();
        result = 31 * result + userId.hashCode();
        result = 31 * result + rating;
        return result;
    }

    @Override
    public String toString() {
        return "Review{"
                + "filmId='" + filmId
                + "\', userId='" + userId
                + "\', rating=" + rating
                + '}';
    }
}
