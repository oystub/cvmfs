#!/bin/bash

URL="http://localhost:1323"

if [ "$1" == "createwish" ]; then
    # CURL with a POST request with JSON data
    curl -X POST -H "Content-Type: application/json" -d '{"uri": "https://gitlab-registry.cern.ch/atlas/athena/athanalysis:24.2.0", "cvmfsRepo": "local.test.repo"}' $URL/wishes
fi