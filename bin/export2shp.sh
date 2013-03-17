#!/bin/sh

EXPORT=$1
OGR2OGR=`which ogr2ogr`

for f in `ls -a ${EXPORT}/*.json`
do
    echo "convert ${f}"
    ${OGR2OGR} -overwrite -F 'ESRI Shapefile' ${f}.shp ${f}
done
