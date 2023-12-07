# Orba

Tools for organizing backfilling activity

## Features

1. generate and batch sql file from a csv
2. auto backup to csv before each execution
3. run your sql automatically

## Quick Guide

1. prepare your csv, the structure have to contains the fields that will be updated, and an "id" column
2. run this command to generate the sql(s)

```sh
go run . gen --source <path to your csv> -t <table name> --size <batch size> --output <output dir>
```

3. run this command to execute the backfill

```sh
go run . run --url postgres://<username>:<password>@<your_url>:<port>/<dbname> --source <output dir from prev step>
```

## Wishlist

1. Concurrent Execution
2. generate create (or maybe upsert) query mode
