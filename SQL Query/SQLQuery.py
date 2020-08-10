class SQLQuery():

    columnList = []
    columnAlias = {}
    tableNames = []
    tableAliases = {}
    tableColumnsDict = {}
    createTable = False
    insertTable = False
    joinClauses = ['join', 'left join', 'inner join', 'full outer join', 'right join']

    def __init__(self, sqlQuery):
        self.sqlQuery = sqlQuery.lower()
        self.cleanQuery()
        self.queryDict = parse(self.sqlQuery)

    #
    # Clean the query before parsing it through the MOZ-SQL-Parser
    # as the parser is not compatible with key words like create,
    # insert, drop etc.
    #
    def cleanQuery(self):
        # Remove extra spaces from the query
        self.sqlQuery = re.sub("\s+", " ", self.sqlQuery)
        if "outer join" in self.sqlQuery:
            self.sqlQuery = self.sqlQuery.replace(" outer join ", " full outer join ")

        # Remove create table clause from the query as moz sql parser cant handle it
        if "create table" in self.sqlQuery:
            regex = "CREATE TABLE (.*?)SELECT"
            matches = re.finditer(regex, self.sqlQuery, re.IGNORECASE)
            for matchNum, match in enumerate(matches, start=1):
                createClause = match.group()
                createClauseWOSelect = createClause.replace(" as ", " ").replace("(", "").split("select")[0]
                splits = createClauseWOSelect.split()
                self.createTableAlias = splits[-1]
                self.createTable = True
                self.sqlQuery = self.sqlQuery.replace(createClause, "")
                if "(" == createClause.split()[-2]:
                    self.sqlQuery = "(select " + self.sqlQuery
                    closingBracketIndex = bracketStringIndex(self.sqlQuery, 0)
                    self.sqlQuery = self.sqlQuery[1:closingBracketIndex].strip()
                else:
                    self.sqlQuery = "select " + self.sqlQuery.strip()

        # Remove insert table clause from the query as moz sql parser cant handle it
        if "insert into" in self.sqlQuery:
            regex = "insert into(.*?)select"
            matches = re.finditer(regex, self.sqlQuery, re.IGNORECASE)
            for matchNum, match in enumerate(matches, start=1):
                insertClause = match.group()
                tempInsertClause = insertClause.replace("(", "").strip().split("select")[0]
                splits = tempInsertClause.split()
                self.insertTableAlias = cleanTableName(splits[-1])
                self.insertTable = True
                self.sqlQuery = self.sqlQuery.replace(insertClause, "")
                if "(" == insertClause.split()[-2]:
                    self.sqlQuery = "(select " + self.sqlQuery
                    closingBracketIndex = bracketStringIndex(self.sqlQuery, 0)
                    self.sqlQuery = self.sqlQuery[1:closingBracketIndex].strip()
                else:
                    self.sqlQuery = "select " + self.sqlQuery.strip()

        # Remove '&' from the query as it shows up during SAS conversion as moz sql parser cant handle it
        regex = "\&(.*?)[\s;]|[)]"
        matches = re.finditer(regex, self.sqlQuery, re.IGNORECASE)
        if matches is not None:
            for matchNum, match in enumerate(matches, start=1):
                word = match.group()
                newWord = word.replace("&", "")
                self.sqlQuery = self.sqlQuery.replace(word, newWord)
        return

    #
    # Identifies all the columns available in select, from, where,
    # groupby and orderby clauses of a sql query and builds a column
    # list along with tableColumnsDict to facilitate query operations
    #
    def identifyColumns(self):
        selectCols = self.queryDict['select']
        for columDetails in selectCols:
            columnAlias = ""
            if "name" in columDetails.keys(): columnAlias = columDetails['name']
            if type(columDetails['value']) == str:
                columnName = columDetails['value']
                addColumnToTable(columnName, self.tableColumnsDict, self.tableAliases, self.tableNames[0], columnAlias)
            elif type(columDetails['value']) == dict:
                exploreDict(list(columDetails.values())[0], self.tableColumnsDict, self.tableAliases, self.tableNames[0], columnAlias)

        fromCols = self.queryDict['from']
        for columDetails in fromCols[1:]:
            columDetails = columDetails['on']
            exploreDict(list(columDetails.values())[0], self.tableColumnsDict, self.tableAliases, self.tableNames[0], "")

        groupCols = self.queryDict['groupby']
        for columDetails in groupCols:
            columnName = columDetails['value']
            # if columnName not in self.columnList: self.columnList.append(columnName)
            addColumnToTable(columnName, self.tableColumnsDict, self.tableAliases, self.tableNames[0], "")

        orderCols = self.queryDict['orderby']
        for columDetails in orderCols:
            columnName = columDetails['value']
            addColumnToTable(columnName, self.tableColumnsDict, self.tableAliases, self.tableNames[0], "")
            # if columnName not in self.columnList: self.columnList.append(columnName)

        whereCols = self.queryDict['where']
        exploreDict(list(whereCols.values())[0], self.tableColumnsDict, self.tableAliases, self.tableNames[0], "")

        tables = self.tableColumnsDict.keys()
        for table in tables:
            columns = list(self.tableColumnsDict[table].keys())
            for column in columns:
                self.columnList.append(column)
        return

    def identifyColumnAlias(self):
        selectCols = self.queryDict['select']
        for columDetails in selectCols:
            columnAlias = ""
            if "name" in columDetails.keys(): columnAlias = columDetails['name']
            if type(columDetails['value']) == str:
                columnName = columDetails['value']
                if columnName not in self.columnAlias.keys(): self.columnAlias[columnName] = columnAlias
            elif type(columDetails['value']) == dict:
                exploreDict(list(columDetails.values())[0], self.columnList)
        return



    #
    # A helper method that helps add a column to the tableColumnsDict
    # to better structure the query internally within this class
    #
    def addColumnToTable(self, tableName, columnName):
        if tableName in self.tableColumnsDict.keys():
            if columnName not in self.tableColumnsDict[tableName]:
                self.tableColumnsDict[tableName].append(columnName)
        else:
            self.tableColumnsDict[tableName] = [columnName]
        return

    #
    # A method to identify all the tables being referred
    # to in the query in from and join clauses
    #
    def identifyTables(self):
        tableDetails = self.queryDict['from']
        for tableDetail in tableDetails:
            tableAlias = ""
            tableName = ""
            if tableDetail == tableDetails[0]:
                tableName = tableDetail['value']
                tableAlias = ""
                if 'name' in tableDetail.keys(): tableAlias = tableDetail['name']
                self.tableNames.append(tableName)
                self.tableAliases[tableAlias] = tableName
            for joinClause in self.joinClauses:
                if joinClause in tableDetail.keys():
                    tableName = tableDetail[joinClause]['value']
                    if 'name' in tableDetail[joinClause].keys(): tableAlias = tableDetail[joinClause]['name']
                    self.tableNames.append(tableName)
                    self.tableAliases[tableAlias] = tableName
        return

    def identifyCaseStatements(self):
        caseStatements = []
        selectCols = self.queryDict['select']
        for col in selectCols:
            if type(col['value']) == dict and "case" in col['value'].keys():
                columnName = col['name']
                caseStatement = col['value']
                statementDict = {caseStatement : columnName}
                caseStatements.append(statementDict)
        return caseStatements

if __name__ == '__main__':

    from moz_sql_parser import parse
    from Utils import *
    import re
    import random as r
    from itertools import groupby

    query = """CREATE TABLE somethingelse AS  ( SELECT marsha, stay_year, coalesce( crossover_rms,0) AS CO_RN_Goal, 
                case when tab1.a=tab2.b then case when tab.e=i0 then true else false end when c=d then true else false end as newcol,
                coalesce( ( crossover_rms*crossover_gadr),0) AS CO_Rev_Goal, def_rms AS Def_OTB
                FROM AW_TGT_BUS_DBO.OY_ANNUALCROSSOVER WHERE stay_year < &YEARNXT3 AND ASOF_YRMO=&CURYRPD 
                ORDER BY marsha, stay_year);"""

    query = """create table newTable SELECT count(B.alpha) as alpha1,
            coalesce( A.crossover_rms,0) as CO_RN_Goal,
            coalesce( ( A.crossover_rms*B.crossover_gadr),0) as CO_Rev_Goal,
            case when a.table_col_1 =b.table_col_2 and table_col_4< table_col_5 or table_col_6>=table_col_7 then True when c.table_col_1< 0 then true else false end as newCol,
            A.marsha as MARS,
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
            on A.marsha = B.marsha and A.marsha1 = B.marsha1
            outer join tableC C
            on A.marsha = C.marsha and A.marsha1 = C.marsha1
            Where A.Target=1
            and C.Target in (1,2,3,4)
            and c.Avg_Bkd="ABCD"
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

    # r.seed(23)

    a = SQLQuery(query)
    # a.queryDict['select']
    for b in a.queryDict['select']:
        print(b)

    # exploreDict()

    # a.identifyTables()
    # print(a.tableNames)
    # print(a.tableAliases)
    # a.identifyColumns()
    # print("column list: ",a.columnList)
    # print("column dict: ",a.tableColumnsDict)