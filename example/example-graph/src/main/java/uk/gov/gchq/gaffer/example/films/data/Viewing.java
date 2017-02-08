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

public class Viewing {
    private String filmId;
    private String userId;
    private long startTime;

    public Viewing() {
    }

    public Viewing(final String filmId, final String userId, final long startTime) {
        this.filmId = filmId;
        this.userId = userId;
        this.startTime = startTime;
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

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(final long startTime) {
        this.startTime = startTime;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof Viewing)) {
            return false;
        }

        final Viewing viewing = (Viewing) o;
        return startTime == viewing.startTime && filmId.equals(viewing.filmId) && userId.equals(viewing.userId);
    }

    @Override
    public int hashCode() {
        int result = filmId.hashCode();
        result = 31 * result + userId.hashCode();
        result = 31 * result + (int) (startTime ^ (startTime >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Viewing{"
                + "userId='" + userId
                + "\', filmId='" + filmId
                + "\', startTime=" + startTime
                + '}';
    }
}
