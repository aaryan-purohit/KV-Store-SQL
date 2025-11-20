### Key Value Store on top of MySql

Personal project to learn how to build Key value store on top of MySQL

### Requirements

* Go - [Download Go](https://go.dev/doc/install)
* MySQL - [Dowmload MySQL](https://www.mysql.com/downloads/)

#### Run following code snippets in MySQL to get the database and table created.

* ```sql
    CREATE DATABASE KV_STORE;
    ```

* ```sql
    CREATE TABLE kv_store_main (
    `key` VARCHAR(255) NOT NULL,
    `value` JSON NOT NULL,
    `expired_at` DATETIME NULL,
    PRIMARY KEY (`key`)
    );
    ```

### How to Run
(database_helper/)
* Run `go mod tidy` to install the dependencies.
* Run `go run main.go` to run the server. By default, it will run on `localhost:8080`. You can view the endpoints exposed in `main.go` under `main()` method.