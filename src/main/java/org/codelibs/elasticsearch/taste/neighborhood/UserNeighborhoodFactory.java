package org.codelibs.elasticsearch.taste.neighborhood;

import java.util.Map;

public interface UserNeighborhoodFactory {

    void init(Map<String, Object> settings);

    UserNeighborhood create();

}