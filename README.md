# Project: Data Modeling with Postgres
Adeeb Alqahtani

A startup company called Sparkify needs a help from a data engineer to help the analyse the data collected through their app. The data engineer assigned job is to create a database schema and ETL pipeline for this analysis.
# Tasks
  - Create a fact and multiple dimention tables.
  - Build ETL Processes
  - Build ETL Pipeline
### Files:
The table below describes all the files needed to complete this project.

| File | Descreption |
| ------ | ------ |
| test.ipynb | displays the first few rows of each table to let you check your database. |
| create_tables.py| drops and creates your tables.  |
| etl.ipynb | This notebook contains detailed instructions on the ETL process for each of the tables. |
| etl.py| reads and processes files from song_data and log_data and loads them into your tables. |
| sql_queries.py | contains all your sql queries, and is imported into the last three files above. |


### Procedure

##### Create Tables
- Write CREATE statements in sql_queries.py to create each table.
- Write DROP statements in sql_queries.py to drop each table if it exists.
- Run create_tables.py to create your database and tables.
- Run test.ipynb to confirm the creation of your tables with the correct columns. Make sure to click "Restart kernel" to close the connection to the database after running this notebook.

##### Build ETL Processes
- Follow instructions in the etl.ipynb notebook to develop ETL processes for each table. 
- Run test.ipynb at the end of each table to confirm that records were successfully inserted. 
- Rerun create_tables.py to reset your tables before each time you run this notebook.

```sh
python create_tables.py
```


##### Build ETL Pipeline
Launch a terminal window and run elt.py. But, you need to run create_tables.py first to reset your tables. The proccess should go like this

```sh
python create_tables.py
python etl.py
```