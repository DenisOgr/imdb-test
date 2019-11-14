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
