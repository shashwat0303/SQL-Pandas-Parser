from SQL_Query.SQLQuery import *
from Utils import *
import re
from moz_sql_parser import parse


class PythonScript():

    def __init__(self, sqlQuery):
        self.sqlQuery = sqlQuery
        self.queryObject = SQLQuery(sqlQuery)
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
            if "*" in columns:
                sqlScript = "select * from " + table
            else:
                sqlScript = "select " + ", ".join(columns) + " from " + table
            script = table + " = pd.read_sql('" + sqlScript + "')"
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
        if type(fromCols) != list:
            fromCols = [fromCols]
        if len(fromCols) > 1:
            baseTable = self.tableNames[0]
            for tableDetails in fromCols[1:]:
                conditionsDict = tableDetails['on']
                leftCols = []
                rightCols = []
                listOfCols = joinStatement(conditionsDict, [])
                print(listOfCols)
                for cols in listOfCols:
                    columnNameA, tableNumA = cleanColumnName(cols[0], self.tableAliases, self.tableColumnsDict,
                                                             self.tableNames)
                    columnNameB, tableNumB = cleanColumnName(cols[1], self.tableAliases, self.tableColumnsDict,
                                                             self.tableNames)
                    if tableNumA < tableNumB:
                        leftCols.append(columnNameA)
                        rightCols.append(columnNameB)
                    else:
                        rightCols.append(columnNameA)
                        leftCols.append(columnNameB)
                for joinClause in self.queryObject.joinClauses:
                    if joinClause in tableDetails.keys():
                        tableName = tableDetails[joinClause]['value']
                        script = baseTable + " = pd.merge(" + baseTable + ", " + tableName + ", how = '" + joinClause + \
                                 "', left_on = " + str(leftCols) + ", right_on = " + str(rightCols) + ")"
                        finalScript.append(script)
        return finalScript

    #
    # This methods filters the pandas dataframe created based on the
    # where clause as mentioned in the SQL Query
    #
    def whereClausePandasDF(self):
        whereScript = []
        baseTable = self.tableNames[0]
        if "where" in self.queryObject.queryDict.keys():
            whereCondition = self.queryObject.queryDict['where']
            script = handleWhereClause(whereCondition, self.queryObject.columnList, self.tableColumnsDict, self.tableNames,
                                       self.tableAliases)
            whereScript = baseTable + " = " + baseTable + ".query('" + script + "')"
        return whereScript

    #
    # This methods groups by the columns of the pandas dataframe
    # as mentioned in the SQL Query
    #
    def groupPandasDFs(self):
        script = ""
        if "grouby" in self.queryObject.queryDict.keys():
            groupCols = self.queryObject.queryDict['groupby']
            columns = []
            baseTable = self.tableNames[0]
            for col in groupCols:
                columnName = col['value']
                updatedColumn, tabelNum = cleanColumnName(columnName, self.tableAliases, self.tableColumnsDict,
                                                          self.tableNames)
                columns.append(updatedColumn)
            script = baseTable + " = " + baseTable + ".groupby(by = " + str(columns) + ")"
        return script

    #
    # This methods orders by the columns of the pandas dataframe
    # as mentioned in the SQL Query
    #
    def orderPandasDFs(self):
        script = ""
        if "orderby" in self.queryObject.queryDict.keys():
            groupCols = self.queryObject.queryDict['orderby']
            columns = []
            baseTable = self.tableNames[0]
            for col in groupCols:
                columnName = col['value']
                updatedColumn, tableNum = cleanColumnName(columnName, self.tableAliases, self.tableColumnsDict,
                                                          self.tableNames)
                columns.append(updatedColumn)
            script = baseTable + ".sort_values(by = " + str(columns) + ", inplace = True)"
        return script

    #
    # This methods identifies nested UDFs if any within the select clause
    # and returns an equivalent pandas script for the same
    #
    def handleUDFStatements(self):
        finalScript = []
        selectCols = self.queryObject.queryDict['select']
        for col in selectCols:
            if type(col) == str:
                col = {'value' : col}
            if type(col['value']) == dict and "case" not in col['value'].keys():
                dataStructure = col
                details = dataStructure['value']
                columnAlias = dataStructure['name']
                udfList = handleUDFs(details, self.queryObject.columnList, columnAlias, self.tableNames, self.tableAliases,
                                     self.tableColumnsDict, [])
                for udf in udfList:
                    key = list(udf.keys())[0]
                    colList = udf[key]
                    script = udfScript(key, colList, self.queryObject.columnList, columnAlias, self.tableNames,
                                       self.tableAliases, self.tableColumnsDict)
                    finalScript.append(script)
        return finalScript

    #
    # This methods identifies case statements and tranverses through the query dict
    # to handle simple and nested case statements
    #
    def handleCaseStatements(self):
        finalScript = []
        baseTable = self.tableNames[0]
        selectCols = self.queryObject.queryDict['select']
        for col in selectCols:
            if type(col) == str:
                col = {'value' : col}
            if type(col['value']) == dict and "case" in col['value'].keys():
                dataStructure = col
                columnAlias = col['name']
                case = dataStructure['value']
                condList, resList = handleCases(case, self.queryObject.columnList, columnAlias, self.tableNames,
                                                self.tableAliases, self.tableColumnsDict, [], [])
                for i in range(len(resList) - 1, -1, -1):
                    if i == len(resList) - 1:
                        script = baseTable + "['" + columnAlias + "'] = " + resList[-1]
                    else:
                        script = baseTable + "['" + columnAlias + "'].iloc[" + condList[i] + "] = " + resList[i]
                    finalScript.append(script)
        return finalScript

    def buildPandasScript(self):
        finalScript = []
        extraLine = ""

        pandasImport = self.importPackage("pandas", 'pd')
        finalScript.append(pandasImport)
        finalScript.append(extraLine)

        readScripts = self.readPandasDFs()
        for readScript in readScripts:
            finalScript.append(readScript)
        finalScript.append(extraLine)

        renameScripts = self.renameColumns()
        if renameScripts != []:
            for renameScript in renameScripts:
                finalScript.append(renameScript)
            finalScript.append(extraLine)

        mergeScripts = self.joinPandasDFs()
        if mergeScripts != []:
            for mergeScript in mergeScripts:
                finalScript.append(mergeScript)
            finalScript.append(extraLine)

        groupScripts = self.groupPandasDFs()
        if groupScripts != "":
            finalScript.append(groupScripts)
            finalScript.append(extraLine)

        udfs = self.handleUDFStatements()
        if udfs != []:
            for udf in udfs:
                finalScript.append(udf)
            finalScript.append(extraLine)

        cases = self.handleCaseStatements()
        if cases != []:
            for case in cases:
                finalScript.append(case)
            finalScript.append(extraLine)

        whereClauses = self.whereClausePandasDF()
        if whereClauses != "":
            finalScript.append(whereClauses)
            finalScript.append(extraLine)

        orderScripts = self.orderPandasDFs()
        if orderScripts != "":
            finalScript.append(orderScripts)
            finalScript.append(extraLine)

        createScripts = self.createTableScript()
        if createScripts != []:
            for createScript in createScripts:
                finalScript.append(createScript)
            finalScript.append(extraLine)

        insertScripts = self.insertTableScript()
        if insertScripts != []:
            for insertScript in insertScripts:
                finalScript.append(insertScript)
            finalScript.append(extraLine)

        return finalScript

    def createTableScript(self):
        finalScript = []
        if self.queryObject.createTable:
            script = self.queryObject.createTableAlias + " = " + self.tableNames[0]
            finalScript.append(script)
            script = "pd.to_sql(" + self.queryObject.createTableAlias + ", con = " + "SQL_ENGINE" + ", if_exists = " + "'replace', index = False)"
            finalScript.append(script)
        return finalScript

    def insertTableScript(self):
        finalScript = []
        if self.queryObject.insertTable:
            script = self.queryObject.insertTableAlias + " = " + self.tableNames[0]
            finalScript.append(script)
            script = "pd.to_sql(" + self.queryObject.insertTableAlias + ", con = " + "SQL_ENGINE" + ", if_exists = " + "'append', index = False)"
            finalScript.append(script)
        return finalScript


if __name__ == '__main__':

    """
    when A.Avg_Bkd=e then case when A.Avg_Bkd=g or A.Avg_Bkd=n then h else i end """

    query = """create table newTable SELECT count(B.alpha) as alpha1,
                coalesce( A.crossover_rms,0) as CO_RN_Goal,
                coalesce( ( sum(A.crossover_rms)*B.crossover_gadr*A.crossover_rms),0) as CO_Rev_Goal,
                case when A.crossover_rms>1 then True when c=d then True else false end as newCol,
                A.marsha as MARS,
                A.crossover_rms as newcolumn,
                c.avg_bkd as renamedColumn,
                A.stay_year as stay_year_REN,
                A.CO_RN_Goal,
                A.CO_Rev_Goal,
                A.CO_RN_Goal_ADR,
                A.Def_OTB,
                A.Def_REV,
                A.Def_ADR,
                A.Target,
                A.Avg_Bkd
                FROM tableA A
                left join tableB B
                on B.marsha = a.marsha and A.marsha1 = B.marsha1 or a.marsha2<=b.marsha2
                outer join tableC C
                on A.marsha = C.marsha and A.marsha1 = C.marsha1
                Where A.Target=1
                and A.Target in (1,2,3,4)
                or c.Avg_Bkd="ABCD" and b.marsha = ""
            	group by
                crossover_rms,
            	crossover_gadr,marsha,
            	stay_year,
            	CO_RN_Goal,
            	CO_Rev_Goal,
            	CO_RN_Goal_ADR,
            	Def_OTB,
            	Def_REV,
            	Def_ADR,
            	Target,
            	Avg_Bkd
            	order by
                A.marsha,A.Avg_Bkd"""


    # query = """CREATE TABLE ABC1 AS (SELECT * , SUM(ABC) as new_col FROM A GROUP BY ALPHA)"""

    # query = """SELECT * , SUM(ABC) as new_col FROM table GROUP BY ALPHA"""

    query = """select sum(col) as a from table"""

    p = PythonScript(query)
    # print(p.sqlQuery)
    # print(p.tableNames)
    # print(p.tableAliases)
    # print(p.tableColumnsDict)

    for a in p.buildPandasScript():
        print(a)

    # print(p.readPandasDFs())
    # for a in p.handleCaseStatements():
    #     print(a)
