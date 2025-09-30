FROM postgis/postgis:16-3.4
RUN apt update && apt install gdal-bin nano -y
COPY ./import*.sh ./opt
