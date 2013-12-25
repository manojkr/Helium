#!/bin/bash

server=$1
home_dir=`pwd`/..
echo "homedir is $home_dir."

java -server -Xms2G -Xms2G -cp "$home_dir/lib/*" -server -Dlog4j.configuration=file:///$home_dir/config/helenusclient.log4j.properties -Dapp.config=$home_dir/config/app.config edu.uiuc.helium.kv.HeliumClientCLI
