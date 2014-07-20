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

package org.codelibs.elasticsearch.taste.recommender;

import java.util.Collection;

import org.codelibs.elasticsearch.taste.common.FastIDSet;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.PreferenceArray;

/**
 * Abstract base implementation for retrieving candidate items to recommend
 */
public abstract class AbstractCandidateItemsStrategy implements
        CandidateItemsStrategy, MostSimilarItemsCandidateItemsStrategy {

    @Override
    public FastIDSet getCandidateItems(final long userID,
            final PreferenceArray preferencesFromUser, final DataModel dataModel) {
        return doGetCandidateItems(preferencesFromUser.getIDs(), dataModel);
    }

    @Override
    public FastIDSet getCandidateItems(final long[] itemIDs,
            final DataModel dataModel) {
        return doGetCandidateItems(itemIDs, dataModel);
    }

    protected abstract FastIDSet doGetCandidateItems(long[] preferredItemIDs,
            DataModel dataModel);

    @Override
    public void refresh(final Collection<Refreshable> alreadyRefreshed) {
    }
}
