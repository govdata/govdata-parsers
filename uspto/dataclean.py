import SQLite

# sort given table in database db by attribute, by default sorts by patent number
def sortTable(db = ':memory:', table = None, attribute = 'patent'):
	s = SQLite.SQLite(db)
	sorted_table = table + "_sort"
	s.c.execute("DROP TABLE IF EXISTS %s" % (sorted_table))
	s.replicate(tableTo = sorted_table, table = table)
	s.c.execute("INSERT INTO %s SELECT * FROM %s ORDER BY %s" % (sorted_table, table, attribute))
	s.commit()
	print("Table " + table + " now sorted by " + attribute + "!")
