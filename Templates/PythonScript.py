from SQL_Query.SQLQuery import *
from Utils import *
import re
from moz_sql_parser import parse


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
        baseTable = self.tableNames[0]
        whereCondition = self.queryObject.queryDict['where']
        script = handleWhereClause(whereCondition, self.queryObject.columnList, self.tableColumnsDict, self.tableNames, self.tableAliases)
        whereScript = baseTable + " = " + baseTable + ".query('" + script + "')"
        return whereScript

    #
    # This methods groups by the columns of the pandas dataframe
    # as mentioned in the SQL Query
    #
    def groupPandasDFs(self):
        groupCols = self.queryObject.queryDict['groupby']
        columns = []
        baseTable = self.tableNames[0]
        for col in groupCols:
            columnName = col['value']
            updatedColumn, tabelNum = cleanColumnName(columnName, self.tableAliases, self.tableColumnsDict, self.tableNames)
            columns.append(updatedColumn)
        script = baseTable + " = " + baseTable + ".groupby(by = " + str(columns) + ")"
        return script

    #
    # This methods orders by the columns of the pandas dataframe
    # as mentioned in the SQL Query
    #
    def orderPandasDFs(self):
        groupCols = self.queryObject.queryDict['orderby']
        columns = []
        baseTable = self.tableNames[0]
        for col in groupCols:
            columnName = col['value']
            updatedColumn, tableNum = cleanColumnName(columnName, self.tableAliases, self.tableColumnsDict, self.tableNames)
            columns.append(updatedColumn)
        script = baseTable + ".sort_values(by = " + str(columns) + ", inplace = True)"
        return script

if __name__ == '__main__':

    query = """create table newTable SELECT count(B.alpha) as alpha1,
                coalesce( A.crossover_rms,0) as CO_RN_Goal,
                coalesce( ( A.crossover_rms*B.crossover_gadr),0) as CO_Rev_Goal,
                case when a.table_col_1 =b.table_col_2 and table_col_4< table_col_5 or table_col_6>=table_col_7 then True when c.table_col_1< 0 then true else false end as newCol,
                A.marsha as MARS,
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
                and C.Target in (1,2,3,4)
                or c.Avg_Bkd="ABCD"
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

    p = PythonScript(query)
    print(p.whereClausePandasDF())