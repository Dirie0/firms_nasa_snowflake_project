# Local Airflow stack. Run these from the airflow/ folder.
.PHONY: init up down restart logs build ps

init:            # first time only: migrate the DB and create the admin user
	docker compose up airflow-init

up:              # build and start the stack in the background
	docker compose up -d --build

down:            # stop and remove the containers
	docker compose down

restart:         # reload after editing .env (a plain restart won't)
	docker compose up -d --force-recreate

logs:            # follow logs from every service
	docker compose logs -f

build:           # rebuild the image after changing dependencies
	docker compose build

ps:              # show container status
	docker compose ps
