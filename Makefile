all: build

local-build:
	cd cmd/dtap; go build; mv dtap ../..

local-build-feeder:
	cd cmd/feeder; go build; mv feeder ../..

build:
	docker build -t mimuret/dtap:latest .
