# housenumberserverAPI

Server API for OSM housenumber evaluation. For client part, see repo regio-osm/housenumberclient

In OpenStreetMap (OSM, see http://openstreetmap.org), the user community input housenumbers worldwide.

With the OSM housenumber validation program on http://regio-osm.de, public available housenumber lists will be checked against the housenumers already stored in OSM.

With this Server API, the client part (see github repo regio-osm/housenumberclient) can retrieve the official housenumber list from a municipality and stores the evaluation result back to the database.

Other functions will follow.

The server API is written in Java, and realized as Java Servlets.

This repo only contains the API part of the server. It's not running standalone. Later, the other part of the server program will be published in another 

public repo within the account regio-osm.
