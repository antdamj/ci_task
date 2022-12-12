# Solutions for entry task for Junior Data Engineer position 

Code solution for all tasks is in ```playersScraper.py``` file. To run the solution, pass a .csv file with players' URLs:

```python playersScraper.py playersURLs.csv```

Please make sure that the .csv file you provide is located in ```/data``` subdirectory.
If none is provided, the script defaults to the one provided together with the task description.

Creation of a database file is also required, using SQLite CLI, i.e. running: ```sqlite3 taskdb```


If, at any moment, you wish to drop and restart the database table, run ```table_reset.py```.

If you wish to see all data stored in the database, run ```table_select.py```.
