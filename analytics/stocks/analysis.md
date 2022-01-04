### Problem Statement

Given a stock data:

- Define a class to keep it
- Have methods to plot its various trends over different timelines and parameters

* So, the initial goal is that of easy visualization first and everything else comes later.

* Before that comes the issue of keeping and retrieving data. DB seems to be a sensible choice
rather than just text and sorting things manually. So, we'll use SQlite3 for this, for now.

[Tutorial](http://www.sqlitetutorial.net/sqlite-python/creating-database/)

DB: bsedata.db

3 Tables are currently defined:

1. List of Equities
2. Historical data of each equity
3. Industry classification of BSE

* With the use of DB come general issues of design. We're using DBs with Objects. So, we would want to
directly operate on objects which would control the DB on their own. (Much like Django and other frameworks).
So, there is a need for two methods at the least - to_db, and to_representation - to convert into and from DB format.
Then, the question would be, why do it? Just use Django or Flask, no? (I'd use Django because I can)

* The problem with trying to write each field after reading it line by line is simply thus: It takes a lot of effing time!

### So we take a U-turn and try to use Django outside Django

And that works!

### So, we would like some visualization

Enter, Jupyter notebooks

- Install Jupyter.
- Fire up a Notebook

```
jupyer notebook
```

- Create a notebook `Stock Visualization`
- Do stuff in it.

- Also, we don't need just an on-demand visualization, but also a programmatic way of visualization that we may embed later into a suite.



# Setting up Django project on a new machine

1. python3 manage.py makemigrations
2. python3 manage.py migrate


