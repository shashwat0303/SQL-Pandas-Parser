from SQL_Query.SQLQuery import SQLQuery


class PythonScript():

    def __init__(self, sqlQuery):
        self.sqlQuery = sqlQuery
        self.queryObject = SQLQuery(sqlQuery)
        self.queryObject.identifyTables()
        self.queryObject.identifyColumns()
        self.tableNames = self.queryObject.tableNames
        self.tableColumnsDict = self.queryObject.tableColumnsDict
        self.tableAliases = self.queryObject.tableAliases

    #
    # This methods imports the relevant packages as will be
    # needed by the python script to process SQL Query
    #
    def importPackage(self, impPkg, alias="", frmPkg=""):
        if frmPkg == "":
            if alias == "":
                return "import " + impPkg
            else:
                return "import " + impPkg + " as " + alias
        else:
            if alias == "":
                return "from " + frmPkg + " import " + impPkg
            else:
                return "from " + frmPkg + " import " + impPkg + " as " + alias

    #
    # This methods reads and imports all the columns needed for the Pandas processing
    # of the given SQL Query
    #
    def readPandasDFs(self):
        finalScript = []
        tables = self.tableColumnsDict.keys()
        for table in tables:
            columns = list(self.tableColumnsDict[table].keys())
            sqlScript = "select " + ", ".join(columns) + " from " + table
            script = table + " = pd.read_sql(" + sqlScript + ")"
            finalScript.append(script)
        return finalScript

    #
    # This methods renames the columns of the respective tables
    # based on the aliases mentioned in the SQL Query
    #
    def renameColumns(self):
        finalScript = []
        tables = self.tableColumnsDict.keys()
        for table in tables:
            columns = list(self.tableColumnsDict[table].keys())
            renameDict = {}
            for column in columns:
                columnAlias = self.tableColumnsDict[table][column]
                if columnAlias != "":
                    renameDict[column] = columnAlias
            script = table + " = " + table + ".rename(columns = " + str(renameDict) + ")"
            finalScript.append(script)
        return finalScript

    #
    # This methods builds the merge script for pandas to join multiple
    # Dataframes together imported from SQL
    #
    def joinPandasDFs(self):
        finalScript = []
        fromCols = self.queryObject.queryDict['from']
        if len(fromCols) > 1:
            baseTable = self.tableNames[0]
            for tableDetails in fromCols[1:]:
                conditionsDict = tableDetails['on']
                leftCols = []
                rightCols = []
                listOfCols = joinStatement(conditionsDict, [])
                for cols in listOfCols:
                    columnNameA, tableNumA = cleanColumnName(cols[0], self.tableAliases, self.tableColumnsDict, self.tableNames)
                    columnNameB, tableNumB = cleanColumnName(cols[1], self.tableAliases, self.tableColumnsDict, self.tableNames)
                    if tableNumA < tableNumB:
                        leftCols.append(columnNameA)
                        rightCols.append(columnNameB)
                    else:
                        rightCols.append(columnNameA)
                        leftCols.append(columnNameB)
                for joinClause in self.queryObject.joinClauses:
                    if joinClause in tableDetails.keys():
                        tableName = tableDetails[joinClause]['value']
                        script = baseTable + " = pd.merge(" + baseTable + ", " + tableName + ", how = " + joinClause +\
                                            ", left_on = " + str(leftCols) + ", right_on = " + str(rightCols) + ")"
                        finalScript.append(script)
        return finalScript

if __name__ == '__main__':
    from Utils import *
    from SQL_Query import SQLQuery
