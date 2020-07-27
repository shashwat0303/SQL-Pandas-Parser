#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 21 10:10:45 2020

@author: skoranne
"""

class SQL_Pandas_Parser():
    tableNames = []
    tableAlias = {}
    caseAlias = {}
    tableColumns = {}
    selectStatements = []
    allCaseStatements = []
    statementsWithoutCases = []
    operators = {" not ":" != ", " equals ":" == ", "eq" : " = ", "neq":" != ", "lte":" <= ", "gte":" >= "}
    script = []
    tableColumnsDict = {}
    keyWords = ['select', 'from', 'join', 'left', 'right',' inner', 'on', 'where', 'order', 'group']

    def __init__(self, sqlQuery):
        self.sqlQuery = sqlQuery.lower()
        if "outer join" in self.sqlQuery:
            self.sqlQuery = self.sqlQuery.replace(" outer join ", " full outer join ")

    def getQueryDict(self):
        return parse(self.sqlQuery.lower())

    def identifyTables(self):
        listOfWords = self.sqlQuery.lower().split()
        index = 0
        for word in listOfWords:
            if word.lower() == "from" or word.lower() == "join":
                tableName = self.cleanTableName(listOfWords[index + 1])
                self.tableNames.append(tableName)
                if index + 2 > len(listOfWords):
                    if listOfWords[index + 2] == "as":
                        self.tableAlias[listOfWords[index + 3]] = tableName
                    elif index + 3 == len(listOfWords):
                        self.tableAlias[listOfWords[index + 2]] = tableName
                    elif listOfWords[index + 3] == "join" or listOfWords[index + 3] == "on":
                        self.tableAlias[listOfWords[index + 2]] = tableName

            index += 1
        return

    def tables(self):
        queryWithTables = self.sqlQuery.split(" from ")[1]
        listOfWords = queryWithTables.split()
        tableName = self.cleanTableName(listOfWords[0])
        self.tableNames.append(tableName)
        self.sqlQuery = self.sqlQuery.replace(listOfWords[0], tableName)
        if len(listOfWords) > 1:
            if listOfWords[1] == "as":
                self.tableAlias[listOfWords[2]] = tableName
            elif listOfWords[1] in self.keyWords:
                pass
            else:
                self.tableAlias[listOfWords[1]] = tableName

        regex = "JOIN (.*?) ON"
        matches = re.finditer(regex, queryWithTables, re.IGNORECASE)
        for matchNum, match in enumerate(matches, start=1):
            match = match.group()
            alias = ""
            tableName = ""
            if "from" in match:
                tableName = self.cleanTableName(match.split()[1])
                self.tableNames.append(tableName)
                self.sqlQuery = self.sqlQuery.replace(match.split()[1], tableName)
                if "join" in self.sqlQuery:
                    tablePhrase = self.sqlQuery.split("from")[1].split("join")[0]
                    alias = tablePhrase.split()[-1]
                else:
                    tablePhrase = self.sqlQuery.split(" from ")
                    splits = tablePhrase.split()
                    if splits[1] == "as":
                        alias = splits[2]
                    else:
                        alias = splits[1]
                # self.tableAlias[alias] = tableName
            elif "join" in match:
                splits = match.split()
                tableName = self.cleanTableName(splits[1])
                self.sqlQuery = self.sqlQuery.replace(splits[1], tableName)
                self.tableNames.append(tableName)
                if splits[2] == "as":
                    alias = splits[3]
                else:
                    alias = splits[2]
            else:
                pass
            self.tableAlias[alias] = tableName
        return

    def cleanTableName(self, tableName):
        if '.' in tableName:
            if '[' in tableName:
                return tableName.split("[")[-1].replace("]", "")
            else:
                return tableName.split(".")[-1]
        return tableName.replace(";", "")

    # =============================================================================
    #     Flatten a list of nested dicts
    # =============================================================================

    def flatten_json(self, nested_json: dict, exclude: list = [''], sep: str = '_') -> dict:
        out = dict()

        def flatten(x: (list, dict, str), name: str = '', exclude=exclude):
            if type(x) is dict:
                for a in x:
                    if a not in exclude:
                        flatten(x[a], f'{name}{a}{sep}')
            elif type(x) is list:
                i = 0
                for a in x:
                    flatten(a, f'{name}{i}{sep}')
                    i += 1
            else:
                out[name[:-1]] = x

        flatten(nested_json)
        return out

    # =============================================================================
    #     Select statement processing
    # =============================================================================

    def selectStatementScript(self):
        self.identifyTables()
        self.identifyCaseStatements()
        self.generateCaseAlias()

        # print(len(self.allCaseStatements))

        sqlWithoutCase = self.getSQLWithoutCase()

        # print(sqlWithoutCase)

        select_dict = parse(sqlWithoutCase)['select']
        sql_dict = a.select_complex(select_dict)
        selectQueries = self.panda_builder("data", sql_dict)

        for caseStatement in self.allCaseStatements:
            caseScript = self.buildCaseQuery(caseStatement)
            for case in caseScript:
                selectQueries.append(case)

        return selectQueries

    def generateCaseAlias(self):
        for caseStatement in self.allCaseStatements:
            splits = self.sqlQuery.split(caseStatement)
            if "from" not in splits[0].lower():
                listOfWords = splits[1].split()
                if listOfWords[0].lower() == "as":
                    self.caseAlias[caseStatement] = listOfWords[1].replace(",", "")
                elif listOfWords[0] == ")":
                    if listOfWords[1].lower() == "as":
                        self.caseAlias[caseStatement] = listOfWords[2].replace(",", "")
                    else:
                        self.caseAlias[caseStatement] = listOfWords[1].replace(",", "")
                else:
                    self.caseAlias[caseStatement] = listOfWords[0].replace(",", "")
            else:
                self.caseAlias[caseStatement] = ""
        return

    def getSQLWithoutCase(self):
        sqlWithoutCase = self.sqlQuery
        for caseStatement in self.allCaseStatements:
            if len(self.caseAlias[caseStatement]) > 0:
                sqlWithoutCase = sqlWithoutCase.replace(caseStatement, "")
                sqlWithoutCase = sqlWithoutCase.replace("()", "")
                alias = self.caseAlias[caseStatement]
                listOfWords = sqlWithoutCase.split()
                index = -1
                for word in listOfWords:
                    if alias == word.replace(",", ""):
                        index = listOfWords.index(word)
                        if listOfWords[index - 1] == "as" or listOfWords[index - 1] == "AS" or listOfWords[
                            index - 1] == "As":
                            listOfWords.pop(index - 1)
                            index = index - 1
                        if listOfWords[index + 1] == ",":
                            listOfWords.pop(index + 1)
                        listOfWords.pop(index)
                    if caseStatement == self.allCaseStatements[-1]:
                        listOfWords[index - 1] = listOfWords[index - 1].replace(",", "")
                sqlWithoutCase = " ".join(listOfWords)
        return sqlWithoutCase

    def buildCaseQuery(self, caseStatement):
        self.identifyTables()
        updatedCaseStatement = self.cleanCaseStatement(caseStatement)
        # print("case: ",caseStatement)
        conditions, results = self.caseStatementDetails(updatedCaseStatement)
        alias = ""
        caseScript = []
        # print(sql)
        # print(caseStatement)
        aliasPhrase = self.sqlQuery.split(caseStatement)[1].strip()
        if aliasPhrase.split(" ")[0] == "as":
            alias = aliasPhrase.split(" ")[1].strip()
            self.caseAlias[caseStatement] = alias
        for i in range(len(conditions)):
            column, table = self.getColumnTableNames(conditions[i])
            if table == "":
                table = self.getTableNames()[0]
            modifiedCondition = conditions[i].split(column, 1)[1]
            for operator in self.operators.keys():
                if operator in modifiedCondition:
                    modifiedCondition = modifiedCondition.replace(operator, self.operators[operator])
            columnCondition = table + "['" + column + "]'" + modifiedCondition
            if alias == "":
                if "." in column:
                    alias = column.split(".")[-1]
                else:
                    alias = column
            if i == 0:
                caseScript.append(table + "['" + alias.replace(",", "") + "'] = " + results[-1])
            caseScript.append(table + ".loc[" + columnCondition + ", '" + alias.replace(",", "") + "'] = " + results[i])
        return caseScript

    def getTableNames(self):
        self.identifyTables()
        return self.tableNames

    def identifyColumns(self):
        regex = "\w+(?:\.\w+)"
        matches = re.finditer(regex, self.sqlQuery, re.IGNORECASE)
        for matchNum, match in enumerate(matches, start=1):
            column = match.group()
            splits = column.split(".")
            tableName = self.tableAlias[splits[0]]
            columnName = splits[1]
            if tableName in self.tableColumnsDict:
                if columnName not in self.tableColumnsDict[tableName]:
                    self.tableColumnsDict[tableName].append(columnName)
            else:
                self.tableColumnsDict[tableName] = [columnName]
        return

    def getColumns(self):
        query_dict = parse(self.sqlQuery)['select']
        if type(query_dict) != list:
            query_dict = [query_dict]
        for columnDetail in query_dict:
            if columnDetail == "*":
                if self.tableNames[0] in self.tableColumnsDict.keys():
                    self.tableColumnsDict[self.tableNames[0]]['*'] = ""
                else:
                    self.tableColumnsDict[self.tableNames[0]] = {"*" : ""}
                continue
            value = columnDetail['value']
            columnDict = {}
            try:
                alias = columnDetail['name']
            except:
                alias = ""
            if type(value) == str:
                if "." in value:
                    tableAlias = value.split(".")[0]
                    tableName = self.tableAlias[tableAlias]
                    columnName = value.split(".")[1]
                    columnDict = {columnName : alias}
                    if tableName in self.tableColumnsDict.keys():
                        self.tableColumnsDict[tableName][columnName] = alias
                    else:
                        self.tableColumnsDict[tableName] = columnDict
                else:
                    columnName = value
                    tableName = self.tableNames[0]
                    if tableName in self.tableColumnsDict.keys():
                        self.tableColumnsDict[tableName][columnName] = alias
                    else:
                        self.tableColumnsDict[tableName] = columnDict
        regex = "\w+(?:\.\w+)"
        matches = re.finditer(regex, self.sqlQuery, re.IGNORECASE)
        for matchNum, match in enumerate(matches, start=1):
            column = match.group()
            splits = column.split(".")
            tableName = self.tableAlias[splits[0]]
            columnName = splits[1]
            columnDict = {columnName : ""}
            if tableName in self.tableColumnsDict.keys():
                if columnName not in self.tableColumnsDict[tableName].keys():
                    self.tableColumnsDict[tableName][columnName] = ""
            else:
                self.tableColumnsDict[tableName] = columnDict
        return

    def selectQuery(self):
        queryScript = []
        self.getColumns()
        for table in self.tableColumnsDict.keys():
            columnDetails = self.tableColumnsDict[table]
            columnNames = list(columnDetails.keys())
            script = table + " = pd.read_sql(select "
            if "*" in columnNames:
                script += "*"
            else:
                for column in columnNames:
                    script += str(column)
                    if column != columnNames[-1]:
                        script += ", "
            script += " from " + str(table) + ")"
            queryScript.append(script)
        return queryScript

    def renameColumns(self):
        queryScript = []
        script = ""
        for table in self.tableColumnsDict:
            columnDetails = self.tableColumnsDict[table]
            columns = list(columnDetails.keys())
            renameDict = {}
            for column in columns:
                newName = columnDetails[column]
                if newName != "":
                    renameDict[column] = newName
            if len(renameDict.keys()) > 0:
                script = table + " = " + table + ".rename(columns = " + str(renameDict) + ")"
                queryScript.append(script)
        return queryScript

    def joinQuery(self):
        queryScript = []
        baseTable = ""
        tableToMerge = ""
        from_dict = parse(self.sqlQuery)['from']
        if type(from_dict) != list:
            from_dict = [from_dict]
        if len(from_dict) > 1:
            for tableDetails in from_dict:
                if "value" in tableDetails.keys():
                    baseTable = tableDetails['value']
                if "join" in tableDetails.keys():
                    joinDetails = tableDetails['join']
                    joinType = "'inner'"
                    tableToMerge = joinDetails['value']
                    joinConditions = tableDetails['on']
                    script = self.handleJoinConditions(baseTable, tableToMerge, joinConditions, joinType)
                    queryScript.append(script)
                elif "left join" in tableDetails.keys():
                    joinDetails = tableDetails['left join']
                    joinType = "'left'"
                    tableToMerge = joinDetails['value']
                    joinConditions = tableDetails['on']
                    script = self.handleJoinConditions(baseTable, tableToMerge, joinConditions, joinType)
                    queryScript.append(script)
                elif "right join" in tableDetails.keys():
                    joinDetails = tableDetails['right join']
                    joinType = "'right'"
                    tableToMerge = joinDetails['value']
                    joinConditions = tableDetails['on']
                    script = self.handleJoinConditions(baseTable, tableToMerge, joinConditions, joinType)
                    queryScript.append(script)
                elif "full outer join" in tableDetails.keys():
                    joinDetails = tableDetails['full outer join']
                    joinType = "'outer'"
                    tableToMerge = joinDetails['value']
                    joinConditions = tableDetails['on']
                    script = self.handleJoinConditions(baseTable, tableToMerge, joinConditions, joinType)
                    queryScript.append(script)
            removeDuplicates = baseTable + " = " + baseTable + ".loc[:, ~" + baseTable + ".columns.duplicated()]"
            queryScript.append(removeDuplicates)
        return queryScript

    def handleJoinConditions(self, baseTable, tableToMerge, joinConditions, joinType):
        if "and" not in joinConditions.keys():
            script = self.handleSingleCondition(baseTable, tableToMerge, joinConditions, joinType)
        else:
            script = self.handleMultipleConditins(baseTable, tableToMerge, joinConditions, joinType)
        return script

    def handleSingleCondition(self, baseTable, tableToMerge, joinConditions, joinType):
        script = baseTable + " = pd.merge(" + baseTable + ", " + tableToMerge + ", how = " + joinType + ", "
        key = list(joinConditions.keys())[0]
        columns = joinConditions[key]
        leftColumn = []
        rightColumn = []
        if len(columns) == 2:
            for i in range(2):
                splits = columns[i].split(".")
                table = self.tableAlias[splits[0]]
                column = splits[1]
                if self.tableColumnsDict[table][column] != "":
                    column = self.tableColumnsDict[table][column]
                if baseTable == table:
                    leftColumn.append(column)
                else:
                    rightColumn.append(column)
            tabScript = "left_on = " + str(leftColumn) + ", right_on = " + str(rightColumn) + ")"
            script += tabScript
        return script

    def handleMultipleConditins(self, baseTable, tableToMerge, joinConditions, joinType):
        script = baseTable + " = pd.merge(" + baseTable + ", " + tableToMerge + ", how = " + joinType + ", "
        conditions = joinConditions['and']
        leftColumns = []
        rightColumns = []
        columnNames = []
        tables = []
        for condition in conditions:
            key = list(condition.keys())[0]
            columns = condition[key]
            for column in columns:
                splits = column.split(".")
                table = self.tableAlias[splits[0]]
                columnName = splits[1]
                if self.tableColumnsDict[table][columnName] != "":
                    columnName = self.tableColumnsDict[table][columnName]
                tables.append(table)
                columnNames.append(columnName)
        i = 0
        while i < len(columnNames):
            if self.tableNames.index(tables[i]) < self.tableNames.index(tables[i+1]):
                leftColumns.append(columnNames[i])
                rightColumns.append(columnNames[i+1])
            else:
                rightColumns.append(columnNames[i])
                leftColumns.append(columnNames[i + 1])
            i = i+2
        script += "left_on = " + str(leftColumns) + ", right_on = " + str(rightColumns) + ")"
        return script

    # def caseQuery(self, caseDict):
    #     script = ""
    #     query_dict = parse(self.sqlQuery)['select']
    #     conditions = []
    #     results = []
    #     if "case" in caseDict['value'].keys():
    #         elseResult = caseDict['value']['case'][-1]
    #         if "name" in caseDict['value'].keys():
    #             columnName = caseDict['value']['name']
    #             for caseCondition in caseDict['value']['case'][:-1]:
    #                 whenCon = caseCondition['when']
    #                 if "and" in whenCon.keys() and len(whenCon.keys()) == 1:
    #                     pass
    #                 elif len(whenCon.keys()) == 1 and list(whenCon.keys())[0] in self.operators:
    #                     key = list(whenCon.keys())[0]
    #                     columns = whenCon[key]
    #                     conditionCols = []
    #                     for column in columns:
    #                         colName = self.getColumnName(column)
    #                         conditionCols.append(colName)
    #                     condition = conditionCols[0] + self.operators[key] + conditionCols[1]
    #                     conditions.append(condition)
    #                 thenCon = caseCondition['then']
    #                 if "case" in thenCon.keys() and len(thenCon.keys()) == 1:
    #
    #     return script
    #
    # def singleCaseQuery(self, caseDict):
    #     caseBody = caseDict['case']
    #     for conditions in caseBody:


    def getColumnName(self, column):
        tableName = ""
        tempColumnName = ""
        column = str(column)
        print("col in columnName", str(column))
        if "." in column:
            splits = column.split(".")
            tableName = self.tableAlias[splits[0]]
            tempColumnName = splits[1]
        else:
            print("col in else", column)
            tableName = self.tableNames[0]
            print("col", column)
            tempColumnName = column
            print("col", column)
        if tempColumnName in self.tableColumnsDict[tableName].keys():
            columnName = self.tableColumnsDict[tableName][tempColumnName]
        else:
            columnName = tempColumnName
        return columnName


    def orderByQuery(self, baseTable):
        script = ""
        query_dict = parse(self.sqlQuery)
        if 'orderby' in query_dict.keys():
            columns = query_dict['orderby']
            if type(columns) != list:
                columns = [columns]
            columnsToSortOn = []
            for columnDetails in columns:
                columnName = columnDetails['value']
                if "." in columnName:
                    splits = columnName.split(".")
                    table = self.tableAlias[splits[0]]
                    columnName = splits[1]
                    if self.tableColumnsDict[table][columnName] != "":
                        columnName = self.tableColumnsDict[table][columnName]
                else:
                    table = self.tableNames[0]
                    if self.tableColumnsDict[table][columnName] != "":
                        columnName = self.tableColumnsDict[table][columnName]
                columnsToSortOn.append(columnName)
            script = baseTable + ".sort_values(by = " + str(columnsToSortOn) + ", inplace = True)"
        return script

    def groupByQuery(self, baseTable):
        script = ""
        query_dict = parse(self.sqlQuery)
        if 'groupby' in query_dict.keys():
            columns = query_dict['groupby']
            if type(columns) != list:
                columns = [columns]
            columnsToSortOn = []
            for columnDetails in columns:
                columnName = columnDetails['value']
                if "." in columnName:
                    splits = columnName.split(".")
                    table = self.tableAlias[splits[0]]
                    columnName = splits[1]
                    if self.tableColumnsDict[table][columnName] != "":
                        columnName = self.tableColumnsDict[table][columnName]
                else:
                    table = self.tableNames[0]
                    if self.tableColumnsDict[table][columnName] != "":
                        columnName = self.tableColumnsDict[table][columnName]
                columnsToSortOn.append(columnName)
            script = baseTable + " = " + baseTable + ".groupby(by = " + str(columnsToSortOn) + ")"
        return script

    def handleWhereClauses(self):
        query_dict = parse(self.sqlQuery)
        queryScript = []
        try:
            whereConditions = query_dict['where']
            baseTable = self.tableNames[0]
            script = baseTable + " = " + baseTable + ".query('"
            print(list(whereConditions.keys()))
            for key in list(whereConditions.keys()):
                if "and" in whereConditions.keys():
                    conditions = whereConditions['and']
                    for condition in conditions:
                        script += self.handleSingleWhereClause(condition)
                        if condition != conditions[-1]:
                            script += " & "
                elif "or" in whereConditions.keys():
                    conditions = whereConditions['and']
                    for condition in conditions:
                        script += self.handleSingleWhereClause(condition)
                        if condition != conditions[-1]:
                            script += " | "
                else:
                    script += self.handleSingleWhereClause(whereConditions)
            script += "')"
            queryScript.append(script)
        except:
            pass
        return queryScript

    def handleSingleWhereClause(self, clause):
        script = ""
        key = list(clause.keys())[0]
        lhsRhs = clause[key]
        for sides in lhsRhs:
            side = self.getColumnName(sides)
            script += side
            if sides != lhsRhs[-1]:
                script += self.operators[key]
        return script

    def cleanCaseStatement(self, caseStatement):
        regex = "\w+(?:\.\w+)"
        matches = re.finditer(regex, caseStatement, re.IGNORECASE)
        self.getTableNames()
        tableName = ""
        for matchNum, match in enumerate(matches, start=1):
            column = match.group()
            if type(column) == str:
                if "." in column:
                    column = " " + column
                    table_alias = column.split(".")[0]
                    if table_alias.strip() in self.tableAlias.keys():
                        tableName = self.tableAlias[table_alias.strip()]
                caseStatement = caseStatement.replace(table_alias, " " + tableName)
        for operator in self.operators.keys():
            if operator in caseStatement:
                caseStatement = caseStatement.replace(operator, self.operators[operator])
        return caseStatement

    def getColumnTableNames(self, condition):
        column = condition.split(" ")[0].strip().replace("[", "").replace("]", "")
        tableName = ""
        columnName = ""
        if "." in column:
            splits = column.split(".")
            tableName = splits[-2]
            columnName = splits[-1]
        else:
            columnName = column
        if tableName in self.tableAlias.keys():
            tableName = self.tableAlias[tableName]
        return columnName, tableName

    def caseStatementDetails(self, caseStatement):
        whenSplits = caseStatement.lower().split("when")[1:]
        conditions = [whenSplit.split("then")[0].strip() for whenSplit in whenSplits]
        results = [whenSplit.split("then")[1].split("else")[0].strip() for whenSplit in whenSplits]
        if "else" in caseStatement:
            lastResult = caseStatement.split("else")[1].split("end")[0].strip()
            results.append(lastResult)
        else:
            results[-1] = results[-1].split("end")[0].strip()
        return conditions, results

    def getCaseStatements(self, sqlQuery):
        caseStatements = []
        regex = r"\bcase\b"
        matches = re.finditer(regex, sqlQuery, re.IGNORECASE)
        cases = []
        for matchNum, match in enumerate(matches, start=1):
            caseWord = match.group()
            cases = sqlQuery.split(caseWord)[1:]
            for case in cases:
                case = "(" + caseWord + case
                endIndex = self.bracketStringIndex(case, 0)
                statement = case[:endIndex + 1]
                caseStatements.append(statement)
        return caseStatements

    def identifyCaseStatements(self):
        regex = "CASE(.*?)END"
        matches = re.finditer(regex, self.sqlQuery, re.IGNORECASE)
        for matchNum, match in enumerate(matches, start=1):
            self.allCaseStatements.append(match.group())
        return

    def bracketStringIndex(self, sql, start):
        dict = {'(': 1, ')': -1}
        indexCount = 0
        sum = 0
        for character in sql.lower():
            if character == '(':
                sum += dict['(']
            elif character == ')':
                sum += dict[')']
                if sum == 0:
                    return indexCount
            indexCount += 1

    # =============================================================================
    #     Write Pandas Script
    # =============================================================================

    def buildPandasScript(self):
        finalScript = []
        emptyLine = ""
        self.tables()
        self.getColumns()
        finalScript.append("import pandas as pd")
        finalScript.append("import re")
        finalScript.append(emptyLine)
        for script in self.selectQuery():
            finalScript.append(script)
        finalScript.append(emptyLine)
        for script in self.renameColumns():
            finalScript.append(script)
        finalScript.append(emptyLine)
        for script in self.joinQuery():
            finalScript.append(script)
        # case functions and UDFs
        finalScript.append(emptyLine)
        groupByScript = self.groupByQuery(self.tableNames[0])
        finalScript.append(groupByScript)
        orderByScript = self.orderByQuery(self.tableNames[0])
        finalScript.append(orderByScript)
        return finalScript

    # =============================================================================
    #     FROM statement processing
    # =============================================================================

if __name__ == "__main__":
    from moz_sql_parser import parse
    import re

    query = """select a aaa, b as bbb, 
                case when a = b then True when t1.m2 = t3.c then case when a = b then true else false end else False end as caseCol, 
                t3.c t3c, t2.x2 as t2x2
               from table1 t1 
               outer join table2 t2 on t1.colA = t2.colB 
                    and t1.col3 = t2.col4 
             right join table3 t3 on t2.col = t3.c 
                    and t4.col2 = t1.col5 
             left join table4 t4 on t4.col2 = t1.col5
               where a=1 and b!=2 or t3.c < 4
               group by b, t3.c
               order by t2.col2, a
               """

    a = SQL_Pandas_Parser(query)
    for s in a.buildPandasScript():
        print(s)