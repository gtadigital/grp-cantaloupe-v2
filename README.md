# SARI Cantaloupe

A Docker configuration of the [Cantaloupe](https://cantaloupe-project.github.io/) IIIF Image Server

## How to use

Prerequisites: [Docker](http://docker.io) including Docker Compose

Copy and (if required) edit the `.env.example`
```sh
cp .env.example .env
```

Some tasks require login credentials for EasyDB. Add them to the `.env` or pass them via command line
```sh
docker compose exec jobs task update-data LOGIN=xxxx PASSWORD=xxxx
```

(optional) Edit configuration stored in `config/cantaloupe.properties`

Run the project with
```sh
docker compose up -d
```

### Running the pipeline

The pipeline can be controlled by the Task runner. When running the pipeline for the first time, run the default task
```sh
docker compose exec jobs task
```

This will run the following tasks in order:
- `update-data`
- `generate-csv`
- `download-assets`

## Tasks

The tasks are defined in the `Taskfile.yml` file.

To list available tasks, run:

```sh
docker compose exec jobs task --list
```

This will output a list of tasks:

```
task: Available tasks for this project:
* default:                 Default task
* download-all-data:       Download digital object xml files from easydb
* download-assets:         Download and process assets from CSV
* generate-csv:            Extract data from XML and generate csv
* update-data:             Update data from EasyDB
```

To run a specific task type `task` followed by the task name, e.g.:

```sh
docker compose exec jobs task update-data
```

`update-data` updates the digital object files in your environment if some already exist, or downloads them all if they do not. To download data from scratch regardless run:

```sh
docker compose exec jobs task download-all-data
```

`generate-csv` extracts identifiers and image download URLs from XML files and saves the results to a CSV file.

`download-assets` downloads and processes assets listed in the generated CSV file, converting the images to TIFF format.

You can manually set the input csv file, the offset and the limit for downloading the assets: 

```sh
docker compose exec jobs task download-assets INPUT_FILE=/path/to/my_csv.csv OFFSET=0 LIMIT=100
```

Otherwise, the default values `INPUT_FILE=/data/csv/id_url_table.csv`, `OFFSET=0`, and `LIMIT=999999` are used.

## Configure Proxy

If Cantaloupe is behind a reverse proxy, CORS settings need to be set in order for it to function correctly with IIIF image viewers. For [our Nginx](https://github.com/swiss-art-research-net/sari-nginx) configuration, create a _location_ overwrite by creating a file in the `vhost.d` directory with the name of the virtual host followed by `_location`. e.g. for https://iiif.swissartresearch.net the file should be called `iiif.swissartresearch.net_location`. Specify the CORS settings in this file, for example as follows:

```
add_header      "Access-Control-Allow-Methods" "GET, OPTIONS" always;
add_header      "Access-Control-Allow-Headers" "Accept,DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range" always;
add_header      "Access-Control-Max-Age" 1728000;
```

The `rs-iiif-mirador` component in Metaphacts/ResearchSpace tends to introduce additional slashes in the URL to an image. To redirect URLs with double slashes, insert the following in the _location_ overwrite:

```
if ($request_uri ~ "^[^?]*?//") {
   add_header   "Access-Control-Allow-Origin" "*" always;
   add_header      "Access-Control-Allow-Methods" "GET, OPTIONS" always;
   add_header      "Access-Control-Allow-Headers" "Accept,DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range" always;
   add_header      "Access-Control-Max-Age" 1728000;
   rewrite "^" $scheme://$host$uri permanent;
}
```

This will rewrite the URL to single slashes and insert a CORS header so that the 301 redirect is followed.