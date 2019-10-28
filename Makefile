start-kudu-docker:
	docker run -h localhost -d --rm --name apache-kudu -p 7051:7051 -p 7050:7050 -p 8051:8051 -p 8050:8050 apache/kudu

stop-kudu-docker:
	docker kill apache-kudu

integration-test: start-kudu-docker
	sbt it:test; $(MAKE) stop-kudu-docker
