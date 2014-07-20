/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.codelibs.elasticsearch.taste.similarity;

import java.util.Collection;

import org.codelibs.elasticsearch.taste.common.Cache;
import org.codelibs.elasticsearch.taste.common.FullRunningAverage;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.common.Retriever;
import org.codelibs.elasticsearch.taste.common.RunningAverage;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.PreferenceArray;

/**
 * <p>
 * Implementations of this interface compute an inferred preference for a user and an item that the user has
 * not expressed any preference for. This might be an average of other preferences scores from that user, for
 * example. This technique is sometimes called "default voting".
 * </p>
 */
public final class AveragingPreferenceInferrer implements PreferenceInferrer {

    private static final Float ZERO = 0.0f;

    private final DataModel dataModel;

    private final Cache<Long, Float> averagePreferenceValue;

    public AveragingPreferenceInferrer(final DataModel dataModel) {
        this.dataModel = dataModel;
        final Retriever<Long, Float> retriever = new PrefRetriever();
        averagePreferenceValue = new Cache<Long, Float>(retriever,
                dataModel.getNumUsers());
        refresh(null);
    }

    @Override
    public float inferPreference(final long userID, final long itemID) {
        return averagePreferenceValue.get(userID);
    }

    @Override
    public void refresh(final Collection<Refreshable> alreadyRefreshed) {
        averagePreferenceValue.clear();
    }

    private final class PrefRetriever implements Retriever<Long, Float> {

        @Override
        public Float get(final Long key) {
            final PreferenceArray prefs = dataModel.getPreferencesFromUser(key);
            final int size = prefs.length();
            if (size == 0) {
                return ZERO;
            }
            final RunningAverage average = new FullRunningAverage();
            for (int i = 0; i < size; i++) {
                average.addDatum(prefs.getValue(i));
            }
            return (float) average.getAverage();
        }
    }

    @Override
    public String toString() {
        return "AveragingPreferenceInferrer";
    }

}
