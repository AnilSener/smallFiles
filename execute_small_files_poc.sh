#!/usr/bin/env bash

./execute_data_collection.sh
rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi
echo "well done!"