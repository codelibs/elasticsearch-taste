package org.codelibs.elasticsearch.taste.neighborhood;

import java.util.Map;

import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;

public interface UserNeighborhoodFactory {

    void init(Map<String, Object> settings);

    UserNeighborhood create();

}