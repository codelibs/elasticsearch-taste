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

package org.codelibs.elasticsearch.taste.neighborhood;

import java.util.Collection;

import org.codelibs.elasticsearch.taste.common.RefreshHelper;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.similarity.UserSimilarity;

import com.google.common.base.Preconditions;

/**
 * <p>
 * Contains methods and resources useful to all classes in this package.
 * </p>
 */
abstract class AbstractUserNeighborhood implements UserNeighborhood {

    private final UserSimilarity userSimilarity;

    private final DataModel dataModel;

    private final double samplingRate;

    private final RefreshHelper refreshHelper;

    AbstractUserNeighborhood(final UserSimilarity userSimilarity,
            final DataModel dataModel, final double samplingRate) {
        Preconditions.checkArgument(userSimilarity != null,
                "userSimilarity is null");
        Preconditions.checkArgument(dataModel != null, "dataModel is null");
        Preconditions.checkArgument(samplingRate > 0.0 && samplingRate <= 1.0,
                "samplingRate must be in (0,1]");
        this.userSimilarity = userSimilarity;
        this.dataModel = dataModel;
        this.samplingRate = samplingRate;
        refreshHelper = new RefreshHelper(null);
        refreshHelper.addDependency(this.dataModel);
        refreshHelper.addDependency(this.userSimilarity);
    }

    final UserSimilarity getUserSimilarity() {
        return userSimilarity;
    }

    final DataModel getDataModel() {
        return dataModel;
    }

    final double getSamplingRate() {
        return samplingRate;
    }

    @Override
    public final void refresh(final Collection<Refreshable> alreadyRefreshed) {
        refreshHelper.refresh(alreadyRefreshed);
    }

}
