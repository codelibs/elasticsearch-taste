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

import java.util.List;

import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.recommender.SimilarUser;

/**
 * <p>
 * Implementations of this interface compute a "neighborhood" of users like a given user. This neighborhood
 * can be used to compute recommendations then.
 * </p>
 */
public interface UserNeighborhood extends Refreshable {

    /**
     * @param userID
     *          ID of user for which a neighborhood will be computed
     * @return IDs of users in the neighborhood
     */
    List<SimilarUser> getUserNeighborhood(long userID);

}
