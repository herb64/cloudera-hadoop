#!/bin/bash
javac -classpath `hadoop classpath` *.java
jar cvf avt.jar *.class

