
class SQL_Parser():

    def __init__(self, sqlQuery):
        self.sqlQuery = str(sqlQuery.lower())#encode('ascii', 'ignore'))
        self.queryDict = parse(self.sqlQuery)


if __name__ == '__main__':

    from moz_sql_parser import parse
    from Utils import cleanTableName, bracketStringIndex
    import re

    query = """SELECT a,b FROM table as A"""
    # print(type(query))

    a = SQL_Parser(query)
    print(type(a.queryDict['select'][0]['value']))