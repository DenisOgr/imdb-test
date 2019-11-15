### IMDB task. Compute TOP 10 actors ordered by avg rating
Using databases from: https://www.imdb.com/interfaces/
### How to install
```
    cd data 
    wget https://datasets.imdbws.com/name.basics.tsv.gz
    gunzip name.basics.tsv.gz

    wget https://datasets.imdbws.com/title.akas.tsv.gz
    gunzip title.akas.tsv.gz

    wget https://datasets.imdbws.com/title.principals.tsv.gz
    gunzip title.principals.tsv.gz

    wget https://datasets.imdbws.com/title.ratings.tsv.gz
    gunzip title.ratings.tsv.gz
```
#### How to run:
```
cd src
python ./result.py

```
#### Results:
```
Juraj Barlek-10.0
Colby Clark-10.0
Lukas Meyer-10.0
Louise Lobo-Clarke-10.0
Karl El Sokhn-10.0
Blakely Austin-10.0
Mackinzie Dae-10.0
Ben Rawlings-10.0
Ashley Glazebrook-10.0
Michael Mesmer-10.0
```
