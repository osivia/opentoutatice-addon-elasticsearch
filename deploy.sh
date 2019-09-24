#! /bin/bash

mvn -f opentoutatice-addon-elasticsearch-ecm/pom.xml clean install \
	&& cp opentoutatice-addon-elasticsearch-marketplace/src/main/resources/install/lib/*.jar /home/sys/docker/volumes/full-es-reindexing-dev_nx-lib/_data \
	&& docker exec -d full-es-reindexing-dev_demo-index-nuxeo_1 /bin/bash chown -R nuxeo:nuxeo /opt/nuxeo/nxserver/lib \
	&& cp -r opentoutatice-addon-elasticsearch-marketplace/src/main/resources/install/templates/* /home/sys/docker/volumes/full-es-reindexing-dev_nx-templates/_data \
	&& docker exec -d full-es-reindexing-dev_demo-index-nuxeo_1 /bin/bash chown -R nuxeo:nuxeo /opt/nuxeo/templates \
	&& cp opentoutatice-addon-elasticsearch-ecm/target/opentoutatice-addon-elasticsearch-ecm-*.jar /home/sys/docker/volumes/full-es-reindexing-dev_nx-plugins/_data \
	&& docker exec -d full-es-reindexing-dev_demo-index-nuxeo_1 /bin/bash chown -R nuxeo:nuxeo /opt/nuxeo/nxserver/plugins \
	&& docker-compose -f /home/david/work/projects/rennes/ws/docker/full-es-reindexing-dev/docker-compose.yml restart demo-index-nuxeo 

#mvn -f opentoutatice-addon-elasticsearch-ecm/pom.xml -o clean install \
#&& cp opentoutatice-addon-elasticsearch-ecm/target/opentoutatice-addon-elasticsearch-ecm-*.jar /home/sys/docker/volumes/full-es-reindexing-dev_nx-plugins/_data \
#	&& docker exec -d full-es-reindexing-dev_demo-index-nuxeo_1 /bin/bash chown -R nuxeo:nuxeo /opt/nuxeo/nxserver/plugins \
#	&& docker-compose -f /home/david/work/projects/rennes/ws/docker/full-es-reindexing-dev/docker-compose.yml restart demo-index-nuxeo 
	
	