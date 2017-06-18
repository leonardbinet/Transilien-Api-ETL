## DATA

There are two sources of data, schedule data and realtime data.

### Schedule Data

It is provided through SNCF data website. It is usually weekly updated and available here:
https://ressources.data.sncf.com/

More specifically, information we use in this application is:
- transilien GTFS schedule: https://ressources.data.sncf.com/explore/dataset/sncf-transilien-gtfs/
- line-based description of Transilien stations: https://ressources.data.sncf.com/explore/dataset/sncf-lignes-par-gares-idf/

These files are no-longer stored in this repository, but instead are downloaded automatically so you can have
fresh data.

When they are downloaded they will be stored in this folder on you computer. 

#### Relational DB
Schedule data is also saved in a relational database (extract_schedule module), so that it becomes easy to query.

### Realtime Data

It is provided through Transilien's API. It provides realtime information which is "nearly instantly" updated.

#### DynamoDB

This information is saved in a Dynamo database.