# load_source_project

Этот репозиторий содержит **Dockerfile** от [apache-airflow](https://github.com/apache/incubator-airflow) для [Docker](https://www.docker.com/)'s [автоматической сборки](https://registry.hub.docker.com/u/puckel/docker-airflow/) опубликованной в общедоступном реестре [Docker Hub Registry](https://registry.hub.docker.com/).

## Информация

* Основано на Python (3.7-slim-buster) официальное изображение [python:3.7-slim-buster](https://hub.docker.com/_/python/) и использует официальный [Postgres](https://hub.docker.com/_/postgres/)
* Скачать [Docker](https://www.docker.com/)
* Скачать [Docker Compose](https://docs.docker.com/compose/install/)

## Предустановка

К сожалению, гит через lfs не может загрузить 1 файл, поэтому скачайте его по этой [ссылке](https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022?select=Combined_Flights_2019.csv) 

## Использование

Проект запускается через **LocalExecutor** :

    docker-compose -f docker-compose-LocalExecutor.yml up -d

Пароль для входа

| Пользователь        | Пароль    |
|---------------------|-----------|
| `airflow`           | `airflow` |

Если вы хотите использовать специальный запрос, убедитесь, что вы настроили подключения:
Перейдите в Admin -> Connections и отредактируйте "postgres_default", установите эти значения (эквивалентно значениям в airflow.cfg/docker-compose-LocalExecutor.yml) :

| Переменная          | Изначальное значение |
|---------------------|----------------------|
| `POSTGRES_HOST`     | `postgres`           | 
| `POSTGRES_PORT`     | `5432`               | 
| `POSTGRES_USER`     | `airflow`            |
| `POSTGRES_PASSWORD` | `airflow`            |
| `POSTGRES_DB`       | `airflow`            |

## Ссылка пользовательского интерфейса

- Airflow: [localhost:8080](http://localhost:8080/)

## Использование HIVE

Для подключения hive к проекту нужно запустить docker-compose с ним, который находится в отдельной папке под названием "hive"

    docker-compose -f hive\docker-compose.yml up -d

Для создания таблиц под датасеты 

    docker exec -it hive-server hive -f /tmp/scripts/hive.hql

Для заполнения данными этих таблиц

    docker exec -it datanode bash /tmp/scripts/linux-script.sh

Для выгрузки датасетов из ods в arhive

    docker exec -it hive-server hive -f /tmp/scripts/load_data_script.hql
