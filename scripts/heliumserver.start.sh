#!/bin/bash


server=$1

home_dir=`pwd`/..
echo "homedir is $home_dir."

java -cp  "$home_dir/lib/*" -server -Xmx2G -Xms2G -Dlog4j.configuration=file:///$home_dir/config/heliumserver$1.log4j.properties -Dapp.config=$home_dir/config/app.config edu.uiuc.helium.kv.HeliumServerCLI

