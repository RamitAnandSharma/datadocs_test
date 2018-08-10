#!/bin/bash
./node_modules/gulp/bin/gulp.js release
mvn -Dmaven.test.skip=true clean install -P release
