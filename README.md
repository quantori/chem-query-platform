# prj-bik-qdp

## Build
`
sbt clean compile
`

## Run
`
sbt run
`

The backend is available by the address:
http://localhost:9000/

### Run frontend
```
docker login  docker.bse.quantori.com
docker-compose up
```

The frontend is available by the address:
http://localhost:8080/

### Backend Package
The following command creates the `target/universal/qdp-0.x.zip` package:
`
sbt dist
`
The package contains scripts in the `bin` folder to run Play application `qdp[.bat]` or CLI `qdp-cli.[bat|sh]`.

Command to create Docker image:
```
docker build -f docker\Dockerfile -t "qdp/play-test" .\target\universal\
```
It can be executed by command like this:
```
docker run -it -p 9000:9000 --tmpfs /tmp qdp/play-test:latest
```