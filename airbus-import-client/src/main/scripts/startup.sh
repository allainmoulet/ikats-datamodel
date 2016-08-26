#!/bin/bash

JAVA_HOME=/opt/jdk1.8.0_45

echo
/opt/jdk1.8.0_45/bin/java -cp .:lib/* fr.cs.ikats.client.temporaldata.importer.AirbusMainClient $*
