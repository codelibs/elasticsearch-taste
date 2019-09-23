Elasticsearch Taste Plugin
=======================

## Overview

Elasticsearch Taste Plugin is [Mahout Taste](https://mahout.apache.org/users/recommender/recommender-documentation.html "Mahout Taste")-based Collaborative Filtering implementation.
This plugin provides the following features of a recommendation engine on Elasticsearch:

* Data management for Users/Items/Preferences.
* Item-based Recommender.
* User-based Recommender.
* Similar Users/Contents
* Text Analysis

## Version

| Taste     | Tested on elasticsearch |
|:---------:|:-----------------------:|
| master    | 2.4.X                   |
| 2.3.0     | 2.3.5                   |
| 1.5.0     | 1.5.1                   |
| 0.4.0     | 1.4.0                   |
| 0.3.1     | 1.3.1                   |

Note that this plugin supports Java 8 or the above.
See [README_river.md](https://github.com/codelibs/elasticsearch-taste/blob/master/README_river.md "README_river.md") under Elasticsearch Taste 0.4.0.

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-taste/issues "issue").
(Japanese forum is [here](https://github.com/codelibs/codelibs-ja-forum "here").)

## Installation

### Install Taste Plugin

    $ $ES_HOME/bin/plugin install org.codelibs/elasticsearch-taste/2.3.0

## Getting Started

### Insert Data

In this section, using [MovieLens](http://grouplens.org/datasets/movielens/ "MovieLens") data set, you can learn about Taste plugin.
For more information about MovieLens, see [MovieLens](http://grouplens.org/datasets/movielens/ "MovieLens") site.
The preference data set is u.data, it contains user id, item id, rating and timestamp.
Download it and then insert data by [Event API](https://github.com/codelibs/elasticsearch-taste/blob/master/README.md#insert-preference-value "Event API"):

    curl -o u.data http://files.grouplens.org/datasets/movielens/ml-100k/u.data
    cat u.data | awk '{system("curl -XPOST localhost:9200/movielens/taste/event?pretty -d \"{\\\"user\\\":{\\\"id\\\":" $1 "},\\\"item\\\":{\\\"id\\\":" $2 "},\\\"value\\\":" $3 ",\\\"timestamp\\\":" $4 "000}\"")}'


By the above request, the preference values are stored in "movielens" index.
After inserting data, check them in the index:

    curl -XGET "localhost:9200/movielens/_search?q=*:*&pretty"

### Recommend Items From Users

To compute recommended items from users, execute the following request:

    curl -XPOST localhost:9200/taste/action/recommended_items_from_user -d '{
      "num_of_items": 10,
      "data_model": {
        "cache": {
          "weight": "100m"
        }
      },
      "index_info": {
        "index": "movielens"
      }
    }'

The result is stored in movielens/recommendation.
The response of the above request has a action name at the "name" property.
To check if your request is finished, send a GET request:

    curl -XGET "localhost:9200/taste/action?pretty"

If the response contains the action name, your request is not completed yet.

For checking the result, send the following query:

    curl -XGET "localhost:9200/movielens/recommendation/_search?q=*:*&pretty"

A value of "user\_id" property is a target user, "items" property is recommended items which contains "item\_id" and "value" property.
A "value" property is a value of similarity.

The computation might take a long time...
If you want to stop it, execute below:

    curl -XDELETE localhost:9200/taste/action/{action_name}

### Evaluate Result

You can evaluate parameters, such as similarity and neighborhood, with the following "evaluate\_items\_from\_user" action.

    curl -XPOST localhost:9200/taste/action/evaluate_items_from_user -d '{
      "evaluation_percentage": 1.0,
      "training_percentage": 0.9,
      "margin_for_error": 1.0,
      "data_model": {
        "cache": {
          "weight": "100m"
        }
      },
      "index_info": {
        "index": "movielens"
      },
      "neighborhood": {
        "factory": "org.codelibs.elasticsearch.taste.neighborhood.NearestNUserNeighborhoodFactory",
        "neighborhood_size": 100
      },
      "evaluator": {
        "id": "movielens_result",
        "factory": "org.codelibs.elasticsearch.taste.eval.RMSEvaluatorFactory"
      }
    }'

The result is stored in report type:

    curl -XGET "localhost:9200/movielens/report/_search?q=evaluator_id:movielens_result&pretty"
    {
      "_index": "movielens",
      "_type": "report",
      "_id": "COscswwlQIugP2Rf0XKH-w",
      "_version": 1,
      "_score": 1,
      "_source": {
        "evaluation": {
          "score": 1.0199981244413419,
          "preference": {
            "total": 10092,
            "no_estimate": 178,
            "success": 6671,
            "failure": 3243
          },
          "time": {
            "average_processing": 18,
            "total_processing": 184813,
            "max_processing": 22881
          }
          "target": {
            "test": 915,
            "training": 943
          }
        }
        "@timestamp": "2014-05-07T21:15:43.948Z",
        "evaluator_id": "movielens_result",
        "report_type": "user_based",
        "config": {
          "training_percentage": 0.9,
          "evaluation_percentage": 1,
          "margin_for_error": 1
        }
      }
    }

### Similar Users

Calcurating similar users, run the following request:

    curl -XPOST localhost:9200/taste/action/similar_users -d '{
      "num_of_users": 10,
      "data_model": {
        "cache": {
          "weight": "100m"
        }
      },
      "index_info": {
        "index": "movielens"
      }
    }'

If you check similar users for User ID 1, type the below:

    curl -XGET "localhost:9200/movielens/user_similarity/_search?q=user_id:1&pretty"

### Get by Your System ID

This plugin has own ID, such as user\_id or item\_id.
If you want to access your system ID, use the following search API:

    curl -XGET "localhost:9200/{index}/{type}/taste/{user|item}/{your_user_or_item_id}?pretty"

For example, if you get similar users from ID=115:

    curl -XGET localhost:9200/movielens/recommendation/taste/user/1

### Create Vectors from Text

You can create a term vector from your index which has a field with "term\_vector".
For the following example, text data are stored into "description" field in "ap" index.

    curl -XPOST localhost:9200/ap?pretty
    curl -XPOST localhost:9200/ap/article/_mapping?pretty -d '{
      "article" : {
        "properties" : {
          "id" : {
            "type" : "string",
            "index" : "not_analyzed"
          },
          "label" : {
            "type" : "string",
            "index" : "not_analyzed"
          },
          "description" : {
            "type" : "string",
            "analyzer" : "standard",
            "term_vector": "with_positions_offsets_payloads",
            "store" : true
          }
        }
      }
    }'
    curl -o ap.txt http://mallet.cs.umass.edu/ap.txt
    sed -e "s/[\'\`\\]//g" ap.txt | awk -F"\t" '{sub(" ","",$1);system("curl -XPOST localhost:9200/ap/article/" $1 " -d \"{\\\"id\\\":\\\"" $1 "\\\",\\\"label\\\":\\\"" $2 "\\\",\\\"description\\\":\\\"" $3 "\\\"}\"")}'

To generate the term vector, run the following river:

    curl -XPOST localhost:9200/taste/action/generate_term_values?pretty -d '{
      "source": {
        "index": "ap",
        "type": "article",
        "fields": ["description"]
      },
      "event": {
        "user_index": "ap_term",
        "user_type": "doc",
        "item_index": "ap_term",
        "item_type": "term",
        "preference_index": "ap_term",
        "preference_type": "preference"
      }
    }'

source property specify index, type and fields as source information, and event property is output information which is user/item/preference format.

## Specification

### Data Management

This plugin manages data of Users, Items and Preferences on Elasticsearch.
These data are stored in each index and type.
By default, they are in:

| Data       | Index | Type       |
|:----------:|:------|:-----------|
| User       | (Any) | user       |
| Item       | (Any) | item       |
| Preference | (Any) | preference |

User index manages id and information about itself.
User index contains the following properties:

| Name       | Type   | Description |
|:----------:|:-------|:------------|
| system\_id | string | Unique ID. If your system has User ID, use it. |
| user\_id   | long   | Unique User ID for this plugin. |
| @timestamp | date   | Created/Updated time. |
| (Any)      | (any)  | You can use other information, such as age, tel,... |

Item index manages an item information and contains:

| Name       | Type   | Description |
|:----------:|:-------|:------------|
| system\_id | string | Unique ID. If your system has Item ID, use it. |
| item\_id   | long   | Unique Item ID for this plugin. |
| @timestamp | date   | Created/Updated time. |
| (Any)      | (any)  | You can use other information, such as price, publish date,... |

Preference index manages values rated by an user for an item.

| Name       | Type   | Description |
|:----------:|:-------|:------------|
| user\_id   | long   | Unique User ID for this plugin. |
| item\_id   | long   | Unique Item ID for this plugin. |
| value      | float  | Value rated by user\_id for item\_id. |
| @timestamp | date   | Created/Updated time. |

You can create/update/delete the above index and document with Elasticsearch manner.

#### Insert Preference Value

Taste plugin provides an useful API to register a preference value.
If you send it to http://.../{index}/\taste/event, user\_id and item\_id are generated and then the preference value is inserted.
You can send multiple data with one request by using carriage return.

For example, if User ID is "U0001", Item ID is "I1000" and the preference(rating) value is 10.0, the request is below:

    curl -XPOST localhost:9200/sample/taste/event -d '{ user: { id: "U0001" }, item: { id: "I1000" }, value: 10.0 }'

If you send multiple data, the request is below:

    curl -XPOST localhost:9200/sample/taste/event -d \
      '{ user: { id: "U0001" }, item: { id: "I1000" }, value: 10.0 }
      { user: { id: "U0002" }, item: { id: "I1001" }, value: 15.0 }'

Values of user\_id, item\_id and @timestamp are generated automatically.
They are stored in "sample" index.
The index name can be changed to any name you want.

### User Recommender

#### Precompute Recommended Items From Users

Recommended items for an user are computed from similar users.
The computing process is started by creating a river configuration.
If 10 recommended items are generated from similar users, the river configuration is:

    curl -XPOST localhost:9200/taste/action/recommended_items_from_user -d '{
      "num_of_items": 10,
      "max_duration": 120,
      "data_model": {
        "class": "org.codelibs.elasticsearch.taste.model.ElasticsearchDataModel",
        "scroll": {
          "size": 1000,
          "keep_alive": 60
        },
        "cache": {
          "weight": "100m"
        }
      },
      "index_info": {
        "preference": {
          "index": "sample",
          "type": "preference"
        },
        "user": {
          "index": "sample",
          "type": "user"
        },
        "item": {
          "index": "sample",
          "type": "item"
        },
        "recommendation": {
          "index": "sample",
          "type": "recommendation"
        },
        "field": {
          "user_id": "user_id",
          "item_id": "item_id",
          "value": "value",
          "items": "items",
          "timestamp": "@timestamp"
        }
      },
      "similarity": {
        "factory": "org.codelibs.elasticsearch.taste.similarity.LogLikelihoodSimilarityFactory"
      },
      "neighborhood": {
        "factory": "org.codelibs.elasticsearch.taste.neighborhood.NearestNUserNeighborhoodFactory",
        "min_similarity": 0.9,
        "neighborhood_size": 10
      }
    }'

The configuration is:

| Name | Type | Description |
|:-----|:-----|:------------|
| num\_of\_items | int | The number of recommended items. |
| max\_duration | int  | Max duration for computing(min). |
| data\_model.class | string | Class name for DataModel implementation. |
| data\_model.scroll | object | Elasticsearch scroll parameters. |
| data\_model.cache | string | Cache size for the data model. |
| index\_info | object | Index information(index/type/property name). |
| similarity.factory | string | Factroy name for Similarity implementation. |
| neighborhood.factory | string | Factroy name for Neighborhood implementation. |

The recommended items are stored in sample/recommendation.
You can see the result by:

    curl -XGET "localhost:9200/sample/recommendation/_search?q=*:*&pretty"

The value of "items" property is recommended items.

#### Evaluate Result

To evaluate parameters for generating recommended items, you can use the following "evaluate\_items\_from\_user" action.

    curl -XPOST localhost:9200/taste/action/evaluate_items_from_user -d '{
      "evaluation_percentage": 0.5,
      "training_percentage": 0.9,
      "data_model": {
        "class": "org.codelibs.elasticsearch.taste.model.ElasticsearchDataModel",
        "scroll": {
          "size": 1000,
          "keep_alive": 60
        },
        "cache": {
          "weight": "100m"
        }
      },
      "index_info": {
        "preference": {
          "index": "sample",
          "type": "preference"
        },
        "user": {
          "index": "sample",
          "type": "user"
        },
        "item": {
          "index": "sample",
          "type": "item"
        },
        "report": {
          "index": "sample",
          "type": "report"
        },
        "field": {
          "user_id": "user_id",
          "item_id": "item_id",
          "value": "value",
          "timestamp": "@timestamp"
        }
      },
      "similarity": {
        "factory": "org.codelibs.elasticsearch.taste.similarity.LogLikelihoodSimilarityFactory"
      },
      "neighborhood": {
        "factory": "org.codelibs.elasticsearch.taste.neighborhood.NearestNUserNeighborhoodFactory"
      }
    }'

| Name | Type | Description |
|:-----|:-----|:------------|
| evaluation\_percentage | float | A sampling rate for a data model. 50% data is used if 0.5, |
| training\_percentage | float | A rate for a training data set of sampled data model. If 0.9, 90% data is used as training one and 10% data is for testing. |

The result is stored in report index(This example is sample/report).

### Item Recommender

#### Precompute Recommended Items From Items

Recommended items for an item are computed from similar items.
The computing process is started by creating a river configuration.
If 10 recommended items are generated from similar items, the river configuration is:

    curl -XPOST localhost:9200/taste/action/recommended_items_from_item -d '{
      "num_of_items": 10,
      "max_duration": 120,
      "data_model": {
        "class": "org.codelibs.elasticsearch.taste.model.ElasticsearchDataModel",
        "scroll": {
          "size": 1000,
          "keep_alive": 60
        },
        "cache": {
          "weight": "100m"
        }
      },
      "index_info": {
        "preference": {
          "index": "sample",
          "type": "preference"
        },
        "user": {
          "index": "sample",
          "type": "user"
        },
        "item": {
          "index": "sample",
          "type": "item"
        },
        "item_similarity": {
          "index": "sample",
          "type": "item_similarity"
        },
        "field": {
          "user_id": "user_id",
          "item_id": "item_id",
          "value": "value",
          "items": "items",
          "timestamp": "@timestamp"
        }
      },
      "similarity": {
        "factory": "org.codelibs.elasticsearch.taste.similarity.LogLikelihoodSimilarityFactory"
      }
    }'

The recommended items are stored in sample/item\_similarity.
You can see the result by:

    curl -XGET "localhost:9200/sample/item_similarity/_search?q=*:*&pretty"

The value of "items" property is recommended items.

