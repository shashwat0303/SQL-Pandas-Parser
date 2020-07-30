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
    createTableAlias = ""
    insertTableAlias = ""
    createTable = False
    insertTable = False
    keyWords = ['select', 'from', 'join', 'left', 'right',' inner', 'on', 'where', 'order', 'group']

    def __init__(self, sqlQuery):
        self.sqlQuery = sqlQuery.lower()
        self.cleanQuery()
        # print("updated query: ", self.sqlQuery)

    def cleanQuery(self):
        # self.sqlQuery = self.sqlQuery.replace("\n", "").strip()
        self.sqlQuery = re.sub("\s+", " ", self.sqlQuery)
        if "outer join" in self.sqlQuery:
            self.sqlQuery = self.sqlQuery.replace(" outer join ", " full outer join ")

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
                    closingBracketIndex = self.bracketStringIndex(self.sqlQuery, 0)
                    self.sqlQuery = self.sqlQuery[1:closingBracketIndex].strip()
                else:
                    self.sqlQuery = "select " + self.sqlQuery.strip()

        if "insert into" in self.sqlQuery:
            regex = "insert into(.*?)select"
            matches = re.finditer(regex, self.sqlQuery, re.IGNORECASE)
            for matchNum, match in enumerate(matches, start=1):
                insertClause = match.group()
                tempInsertClause = insertClause.replace("(", "").strip().split("select")[0]
                splits = tempInsertClause.split()
                self.insertTableAlias = splits[-1]
                self.insertTable = True
                self.sqlQuery = self.sqlQuery.replace(insertClause, "")
                if "(" == insertClause.split()[-2]:
                    self.sqlQuery = "(select " + self.sqlQuery
                    closingBracketIndex = self.bracketStringIndex(self.sqlQuery, 0)
                    self.sqlQuery = self.sqlQuery[1:closingBracketIndex].strip()
                else:
                    self.sqlQuery = "select " + self.sqlQuery.strip()

        regex = "\&(.*?)[\s;]|[)]"
        matches = re.finditer(regex, self.sqlQuery, re.IGNORECASE)
        if matches is not None:
            for matchNum, match in enumerate(matches, start=1):
                word = match.group()
                newWord = word.replace("&", "")
                self.sqlQuery = self.sqlQuery.replace(word,newWord)
        return

    def getQueryDict(self):
        return parse(self.sqlQuery.lower())

    def identifyTables(self):
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
            # print("one table:", match)
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
        sqlWithoutCase = self.getSQLWithoutCase()
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
        conditions, results = self.caseStatementDetails(updatedCaseStatement)
        alias = ""
        caseScript = []
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

    # def identifyColumns(self):
    #     query_dict = parse(self.sqlQuery)['select']
    #     if type(query_dict) != list:
    #         query_dict = [query_dict]
    #     # print("tables:",self.tableNames)
    #     # print("dict:",self.tableColumnsDict)
    #     for columnDetail in query_dict:
    #         if columnDetail == "*":
    #             if self.tableNames[0] in self.tableColumnsDict.keys():
    #                 self.tableColumnsDict[self.tableNames[0]]['*'] = ""
    #             else:
    #                 self.tableColumnsDict[self.tableNames[0]] = {"*" : ""}
    #             continue
    #         value = columnDetail['value']
    #         try:
    #             alias = columnDetail['name']
    #         except:
    #             alias = ""
    #         if type(value) == str:
    #             if "." in value:
    #                 tableAlias = value.split(".")[0]
    #                 tableName = self.tableAlias[tableAlias]
    #                 columnName = value.split(".")[1]
    #                 columnDict = {columnName : alias}
    #                 if tableName in self.tableColumnsDict.keys():
    #                     self.tableColumnsDict[tableName][columnName] = alias
    #                 else:
    #                     self.tableColumnsDict[tableName] = columnDict
    #             else:
    #                 columnName = value
    #                 tableName = self.tableNames[0]
    #                 columnDict = {columnName: alias}
    #                 if tableName in self.tableColumnsDict.keys():
    #                     self.tableColumnsDict[tableName][columnName] = alias
    #                 else:
    #                     self.tableColumnsDict[tableName] = columnDict
    #     regex = "\w+(?:\.\w+)"
    #     matches = re.finditer(regex, self.sqlQuery, re.IGNORECASE)
    #     for matchNum, match in enumerate(matches, start=1):
    #         column = match.group()
    #         splits = column.split(".")
    #         tableName = self.tableAlias[splits[0]]
    #         columnName = splits[1]
    #         columnDict = {columnName : ""}
    #         if tableName in self.tableColumnsDict.keys():
    #             if columnName not in self.tableColumnsDict[tableName].keys():
    #                 self.tableColumnsDict[tableName][columnName] = ""
    #         else:
    #             self.tableColumnsDict[tableName] = columnDict
    #     # print("tab alias:", self.tableAlias)
    #     self.intermediate_select_dict()
    #     # print("dict:", self.tableColumnsDict)
    #     return

    def identifyColumns(self):
        column_list = []
        query_dict = parse(self.sqlQuery)['select']
        if type(query_dict) != list:
            query_dict = [query_dict]
        for columnDetail in query_dict:
            if columnDetail == "*":
                if self.tableNames[0] in self.tableColumnsDict.keys():
                    self.tableColumnsDict[self.tableNames[0]]['*'] = ""
                else:
                    self.tableColumnsDict[self.tableNames[0]] = {"*": ""}
                continue
            value = columnDetail['value']
            try:
                alias = columnDetail['name']
            except:
                alias = ""
            if type(value) == str:
                if "." in value:
                    tableAlias = value.split(".")[0]
                    tableName = self.tableAlias[tableAlias]
                    columnName = value.split(".")[1]
                    columnDict = {columnName: alias}
                    if tableName in self.tableColumnsDict.keys():
                        self.tableColumnsDict[tableName][columnName] = alias
                    else:
                        self.tableColumnsDict[tableName] = columnDict
                else:
                    columnName = value
                    tableName = self.tableNames[0]
                    columnDict = {columnName: alias}
                    if tableName in self.tableColumnsDict.keys():
                        self.tableColumnsDict[tableName][columnName] = alias
                    else:
                        self.tableColumnsDict[tableName] = columnDict

            elif type(value) == dict:
                if '.' in value:
                    column_value = value.split('.')[1]
                    column_table = value.split('.')[0]
                    # try:
                    #     alias = column_dict['name']
                    # except:
                    #     alias = ""
                    tableName = self.tableAlias[column_table]
                    colsttmp = {"base_col": column_value, "Table": column_table, "Alias": alias, "table": tableName}
                    column_list.append(colsttmp)
                else:
                    column_value = value
                    final_col = []
                    for k, v in value.items():
                        if k == "case":
                            pass
                        else:
                            udf = k
                            cols = v
                            if type(cols) == str:
                                if '.' in cols:
                                    col_name = cols.split('.')
                                    final_col.append(col_name[1])
                                else:
                                    final_col.append(cols)
                            elif type(cols) == dict:
                                for k, v in cols.items():  ###########needs to be coded
                                    udf = udf + "," + k
                                    cols = v
                                    for i in cols:
                                        final_col.append(i)

                            else:
                                for i in cols:
                                    if type(i) == str:
                                        if '.' in i:
                                            column_name = i.split('.')
                                            col_name = column_name[1]
                                            final_col.append(col_name)
                                        else:
                                            final_col.append(i)

                                    elif type(i) == int:
                                        final_col.append(i)

                                    elif type(i) == dict:  ## here adjustments needs to be done
                                        new_dict = i
                                        for k, v in new_dict.items():
                                            extra_udf = k
                                            udf = udf + "," + extra_udf
                                            cols = v
                                            if type(cols) == list:  ## for list
                                                for i in cols:
                                                    if '.' in i:
                                                        splitter = i.split('.')
                                                        part1 = splitter[0]
                                                        part2 = splitter[1]
                                                        final_col.append(part2)
                                                    else:
                                                        final_col.append(i)
                                            elif type(cols) == str:  ## for str
                                                if '.' in cols:
                                                    splitter = cols.split('.')
                                                    part1 = splitter[0]
                                                    part2 = splitter[1]
                                                    final_col.append(part2)
                                                else:
                                                    final_col.append(cols)
                                            elif type(cols) == dict:  ## for dict
                                                for k, v in cols.items():
                                                    third_udf = k
                                                    udf = udf + "," + third_udf
                                                    cols = v
                                                    for i in cols:
                                                        if '.' in i:
                                                            splitter = i.split('.')
                                                            part1 = splitter[0]
                                                            part2 = splitter[1]
                                                            final_col.append(part2)
                                                        else:
                                                            final_col.append(i)
                                            else:
                                                pass

                                    else:
                                        pass
                        # try:
                        #     alias = column_dict['name']
                        # except:
                        #     alias = ""
                        colltmp = {"base_col": final_col, "udf": udf, "Alias": alias, "table": self.tableNames[0]}
                        column_list.append(colltmp)

        regex = "\w+(?:\.\w+)"
        matches = re.finditer(regex, self.sqlQuery, re.IGNORECASE)
        for matchNum, match in enumerate(matches, start=1):
            column = match.group()
            splits = column.split(".")
            tableName = self.tableAlias[splits[0]]
            columnName = splits[1]
            columnDict = {columnName: ""}
            if tableName in self.tableColumnsDict.keys():
                if columnName not in self.tableColumnsDict[tableName].keys():
                    self.tableColumnsDict[tableName][columnName] = ""
            else:
                self.tableColumnsDict[tableName] = columnDict
        self.intermediate_select_dict()
        return column_list

    def selectQuery(self):
        queryScript = []
        # self.getColumns()
        for table in self.tableColumnsDict.keys():
            columnDetails = self.tableColumnsDict[table]
            columnNames = list(columnDetails.keys())
            script = table + " = pd.read_sql('select "
            if "*" in columnNames:
                script += "*"
            else:
                for column in columnNames:
                    script += str(column)
                    if column != columnNames[-1]:
                        script += ", "
            script += " from " + str(table) + "')"
            queryScript.append(script)
        return queryScript

    def renameColumns(self):
        queryScript = []
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
        column = str(column)
        if "." in column:
            splits = column.split(".")
            tableName = self.tableAlias[splits[0]]
            tempColumnName = splits[1]
        else:
            tableName = self.tableNames[0]
            tempColumnName = column
        if tempColumnName in self.tableColumnsDict[tableName].keys():
            tempName = self.tableColumnsDict[tableName][tempColumnName]
            if tempName == "":
                columnName = tempColumnName
            else:
                columnName = tempName
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
                # else:
                #     table = self.tableNames[0]
                #     print("table to look into:", table)
                #     if self.tableColumnsDict[table][columnName] != "":
                #     columnName = self.tableColumnsDict[table][columnName]
                columnsToSortOn.append(columnName)
            script = baseTable + " = " + baseTable + ".groupby(by = " + str(columnsToSortOn) + ")"
        return script

    def handleWhereClauses(self):
        baseTable = self.tableNames[0]
        script = ""
        if " where " in self.sqlQuery:
            whereClause = self.sqlQuery.split(' where ')[1]
            keyWords = [' order ', ' group ', ' left ', ' right ', ' inner ', ' full outer ']
            tempWhereClause = ""
            matchCount = 0
            for word in keyWords:
                if word in whereClause:
                    newTemp = whereClause.split(word)[0]
                    c = tempWhereClause  == "" and len(newTemp) > len(tempWhereClause)
                    matchCount += 1
                    if tempWhereClause != "" and len(newTemp) < len(tempWhereClause):
                        tempWhereClause = newTemp
                    elif tempWhereClause  == "" and len(newTemp) > len(tempWhereClause):
                        tempWhereClause = newTemp
            tempWhereClause = tempWhereClause.replace(" and", " &").replace(" or", " | ").replace(" not", " ~")
            listOfWords = tempWhereClause.split()
            for word in listOfWords:
                if "." in word:
                    columnName = self.getColumnName(word)
                    index = listOfWords.index(word)
                    listOfWords[index] = columnName
            finalWhereClause = " ".join(listOfWords)
            script = baseTable + " = " + baseTable + ".query('" + finalWhereClause + "')"
        return script

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
        return indexCount

    def createTableQuery(self):
        queryScript = []
        if self.createTable == True:
            baseTable = self.tableNames[0]
            script = self.createTableAlias + " = " + baseTable
            toSQLScript = "pd.to_sql(" + self.createTableAlias + ", con = " + "SQL_ENGINE" + ", if_exists = 'replace', index = False)"
            queryScript.append(script)
            queryScript.append(toSQLScript)
        return queryScript

    def insertTableQuery(self):
        queryScript = []
        if self.insertTable == True:
            baseTable = self.tableNames[0]
            script = self.insertTableAlias + " = " + baseTable
            toSQLScript = "pd.to_sql(" + self.insertTableAlias + ", con = " + "SQL_ENGINE" + ", if_exists = 'append', index = False)"
            queryScript.append(script)
            queryScript.append(toSQLScript)
        return queryScript

    def handleUDFs(self, sql_dict):
        query_list = []
        grp_cols = []
        query_dict = parse(self.sqlQuery)
        if 'groupby' in query_dict.keys():
            group_section = query_dict['groupby']
            if type(group_section) == list:
                for i in group_section:
                    values = i['value']
                    grp_cols.append(values)
            elif type(group_section) == dict:
                for k, v in group_section.items():
                    grp_cols.append(v)
        else:
            pass
        for list_elements in sql_dict:
            columns = list_elements['base_col']
            if columns != '*':
                if columns != []:
                    final_columns = [s for s in columns if type(s) == str]
                    alias = list_elements['Alias']
                    udf = list_elements['udf']
                    tableName = self.tableNames[0]
                    if list_elements['udf'] != '':
                        udf_splitter = list_elements['udf'].split(',')
                        columnNames = list_elements['base_col']
                        len_udf = len(udf_splitter)
                        if len_udf == 1 and udf == "coalesce":
                            query = self.coalesce_udf(columns, tableName, alias, final_columns, len_udf)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "mul":
                            query = self.multiplication_udf(final_columns, alias, tableName)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "sum":
                            # query= sum_initial_udf(columns,final_df,alias)
                            # query_list.append(query)
                            query = self.group_by_func(tableName, query_dict, grp_cols, alias, udf, final_columns)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "year":
                            query = self.year_month_udf(tableName, alias, udf, final_columns)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "month":
                            query = self.year_month_udf(tableName, alias, udf, final_columns)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "literal":
                            query = self.literals_adjust(tableName, columns, alias)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "distinct":
                            query = self.distinct_unique(tableName, alias, udf, final_columns)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "count":
                            query = self.sum_initial_udf(columns, tableName, alias)
                            query_list.append(query)
                            query = self.group_by_func(tableName, query_dict, grp_cols, alias, udf, final_columns)
                            query_list.append(query)
                        elif len_udf > 1:
                            for udf in reversed(udf_splitter):
                                if udf == 'mul':
                                    query = self.multiplication_udf(final_columns, alias, tableName)
                                    query_list.append(query)
                                elif udf == 'sum':
                                    query = self.sum_initial_udf(columns, tableName, alias)
                                    query_list.append(query)
                                    query = self.group_by_func(tableName, query_dict, grp_cols, alias, udf, final_columns)
                                    # final_df, query_dict, grp_cols, alias, columns, udf, final_columns
                                    query_list.append(query)
                                elif udf == "coalesce":
                                    query = self.coalesce_udf(columns, tableName, alias, final_columns, len_udf)
                                    query_list.append(query)
                        # else:
                        #     columns =list_elements['base_col']
                        #     alias=list_elements['Alias']
                        #     if alias!='':
                        #         query=final_df+"['"+alias+"']"+"="+final_df+"['"+columns+"']"
                        #         query_list.append(query)

                        else:
                            pass
            else:
                pass
        return query_list

    def coalesce_udf(self, columns, final_df, alias, final_columns, len_udf):
        coalesce_filler = str(columns[-1])
        if len_udf == 1:
            query = final_df + "['" + alias + "']" + "=" + final_df + "." + final_columns[
                0] + ".fillna(value=" + coalesce_filler + ',inplace=True)'
        else:
            query = final_df + "['" + alias + "']" + "=" + final_df + "." + alias + ".fillna(value=" + coalesce_filler + ',inplace=True)'
        return query

    # #### Multiplication

    # In[4]:

    def multiplication_udf(self, final_columns, alias, final_df):
        list_of_col = ["row." + a for a in final_columns]
        cols = '*'.join(list_of_col)
        query = final_df + "['" + alias + "']" + "=" + final_df + '.apply(lambda row: ' + cols + ', axis = 1)'
        return query

    # #### basic sum functionality of query

    # In[5]:

    def sum_initial_udf(self, columns, final_df, alias):
        columns = columns[0]
        query = final_df + "['" + alias + "']" + "=" + final_df + "['" + columns + "']"
        return query

    # In[6]:

    # def sum_initial_udf(columns,final_df,alias):
    #     columns=columns[0]
    #     query= final_df+"['"+alias+"']"+"="+final_df+"['"+columns+"']"
    #     return query

    # In[ ]:

    # #### Year and month udf's are being handled here

    # In[7]:

    def group_by_func(self, final_df, query_dict, grp_cols, alias, udf, final_columns):
        # column = columns[0]
        grp_by = ""
        if 'groupby' in query_dict.keys():
            final_fcol = final_columns[0]
            grp_by = final_df + "['" + alias + "']" + "=" + final_df + ".groupby(" + str(
                grp_cols) + ")" + "['" + final_fcol + "']" + ".agg(" + udf + ")"
        return grp_by


    def year_month_udf(self, final_df, alias, udf, final_columns):
        query = final_df + "['" + alias + "']" + "=" + final_df + "['" + final_columns[0] + "']" + ".dt." + udf + ")"
        return query

    # #### Literal

    # In[8]:

    def literals_adjust(self, final_df, columns, alias):
        """add new column to pandas dataframe with default value"""
        columns = columns[0]
        query = final_df + "['" + alias + "']" + "=" + "'" + columns + "'"
        return query

    # #### Distinct or unique

    # In[9]:

    def distinct_unique(self, final_df, alias, udf, final_columns):
        if alias == "":
            final_fcol = final_columns[0]
            query = final_df + "['" + final_fcol + "']" + "=" + final_df + "['" + final_fcol + "'].unique()"
        else:
            final_fcol = final_columns[0]
            query = final_df + "['" + alias + "']" + "=" + final_df + "['" + final_fcol + "'].unique()"
        return query

    def grouped_columns(self):
        list1 = []
        query_dict = parse(self.sqlQuery)
        # if "group by" in query_dict.keys():
        if 'groupby' in query_dict.keys():
            group_section = query_dict['groupby']
            if type(group_section) == list:
                for i in group_section:
                    values = i['value']
                    list1.append(values)
            elif type(group_section) == dict:
                for k, v in group_section.items():
                    list1.append(v)
        else:
            pass
        return list1



    # =============================================================================
    #     Write Pandas Script
    # =============================================================================

    def buildPandasScript(self):
        finalScript = []
        emptyLine = ""
        self.identifyTables()
        column_list = self.identifyColumns()

        # try:
        finalScript.append("import pandas as pd")
        # finalScript.append("import re")

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

        for script in self.handleUDFs(column_list):
            finalScript.append(script)

        finalScript.append(emptyLine)
        whereClaseScript = self.handleWhereClauses()
        finalScript.append(whereClaseScript)

        groupByScript = self.groupByQuery(self.tableNames[0])
        finalScript.append(groupByScript)

        orderByScript = self.orderByQuery(self.tableNames[0])
        finalScript.append(orderByScript)
        finalScript.append(emptyLine)

        for script in self.createTableQuery():
            finalScript.append(script)

        for script in self.insertTableQuery():
            finalScript.append(script)
        # except:
            # return finalScript

        return finalScript

    def intermediate_select_dict(self):
        column_list = []
        select_list = parse(self.sqlQuery)['select']
        for column_dict in select_list:
            if column_dict == "*":
                pass
            elif type(column_dict['value']) == str:
                if '.' in column_dict['value']:
                    column_value = column_dict['value'].split('.')[1]
                    column_table = column_dict['value'].split('.')[0]
                    try:
                        alias = column_dict['name']
                    except:
                        alias = ""
                    tableName = self.tableAlias[column_table]
                    collsttmp = {"base_col": column_value, "udf": "", "Alias": alias, "table": tableName}
                    column_list.append(collsttmp)
                    if column_value not in self.tableColumnsDict[tableName].keys():
                        self.tableColumnsDict[tableName][column_value] = alias
                else:
                    column_value = column_dict['value']
                    column_table = ""
                    try:
                        alias = column_dict['name']
                    except:
                        alias = ""
                    collsttmp = {"base_col": column_value, "udf": "", "Alias": alias, "table": self.tableNames[0]}
                    tableName = self.tableNames[0]
                    if column_value not in self.tableColumnsDict[tableName].keys():
                        self.tableColumnsDict[tableName][column_value] = alias
                    column_list.append(collsttmp)


            elif type(column_dict['value']) == dict:
                if '.' in column_dict['value']:
                    column_value = column_dict['value'].split('.')[1]
                    column_table = column_dict['value'].split('.')[0]
                    try:
                        alias = column_dict['name']
                    except:
                        alias = ""
                    tableName = self.tableAlias[column_table]
                    colsttmp = {"base_col": column_value, "Table": column_table, "Alias": alias, "table": column_table}
                    if column_value not in self.tableColumnsDict[column_table].keys():
                        self.tableColumnsDict[column_table][column_value] = alias
                    column_list.append(colsttmp)
                else:
                    column_value = column_dict['value']
                    final_col = []
                    for k, v in column_dict['value'].items():
                        if k == "case":
                            pass
                        else:
                            udf = k
                            cols = v
                            if type(cols) == str:
                                if '.' in cols:
                                    col_name = cols.split('.')
                                    final_col.append(col_name[1])
                                    tableName = self.tableAlias[col_name[0]]
                                    if col_name[1] not in self.tableColumnsDict[tableName].keys():
                                        self.tableColumnsDict[tableName][col_name[1]] = ""
                                else:
                                    final_col.append(cols)
                                    if cols not in self.tableColumnsDict[self.tableNames[0]].keys():
                                        self.tableColumnsDict[self.tableNames[0]][cols] = ""
                            elif type(cols) == dict:
                                for k, v in cols.items():  ###########needs to be coded
                                    udf = udf + "," + k
                                    for i in v:
                                        final_col.append(i)
                                        if i not in self.tableColumnsDict[self.tableNames[0]].keys():
                                            self.tableColumnsDict[self.tableNames[0]][i] = ""

                            else:
                                for i in cols:
                                    if type(i) == str:
                                        if '.' in i:
                                            column_name = i.split('.')
                                            col_name = column_name[1]
                                            final_col.append(col_name)
                                            tableName = self.tableAlias[col_name[0]]
                                            if col_name[1] not in self.tableColumnsDict[tableName].keys():
                                                self.tableColumnsDict[tableName][col_name[1]] = ""
                                        else:
                                            final_col.append(i)
                                            if i not in self.tableColumnsDict[self.tableNames[0]].keys():
                                                self.tableColumnsDict[self.tableNames[0]][i] = ""

                                    elif type(i) == int:
                                        final_col.append(i)
                                        if i not in self.tableColumnsDict[self.tableNames[0]].keys():
                                            self.tableColumnsDict[self.tableNames[0]][i] = ""

                                    elif type(i) == dict:  ## here adjustments needs to be done
                                        new_dict = i
                                        for k, v in new_dict.items():
                                            extra_udf = k
                                            udf = udf + "," + extra_udf
                                            cols = v
                                            if type(cols) == list:  ## for list
                                                for i in cols:
                                                    if '.' in i:
                                                        splitter = i.split('.')
                                                        part1 = splitter[0]
                                                        part2 = splitter[1]
                                                        final_col.append(part2)
                                                        tableName = self.tableAlias[part1]
                                                        if part2 not in self.tableColumnsDict[tableName].keys():
                                                            self.tableColumnsDict[tableName][part2] = ""
                                                    else:
                                                        final_col.append(i)
                                                        if i not in self.tableColumnsDict[self.tableNames[0]].keys():
                                                            self.tableColumnsDict[self.tableNames[0]][i] = ""
                                            elif type(cols) == str:  ## for str
                                                if '.' in cols:
                                                    splitter = cols.split('.')
                                                    part1 = splitter[0]
                                                    part2 = splitter[1]
                                                    final_col.append(part2)
                                                    tableName = self.tableAlias[part1]
                                                    if part2 not in self.tableColumnsDict[tableName].keys():
                                                        self.tableColumnsDict[tableName][part2] = ""
                                                else:
                                                    final_col.append(cols)
                                                    if cols not in self.tableColumnsDict[self.tableNames[0]].keys():
                                                        self.tableColumnsDict[self.tableNames[0]][cols] = ""
                                            elif type(cols) == dict:  ## for dict
                                                for k, v in cols.items():
                                                    third_udf = k
                                                    udf = udf + "," + third_udf
                                                    cols = v
                                                    for i in cols:
                                                        if '.' in i:
                                                            splitter = i.split('.')
                                                            part1 = splitter[0]
                                                            part2 = splitter[1]
                                                            final_col.append(part2)
                                                            tableName = self.tableAlias[part1]
                                                            if part2 not in self.tableColumnsDict[tableName].keys():
                                                                self.tableColumnsDict[tableName][part2] = ""
                                                        else:
                                                            final_col.append(i)
                                                            if i not in self.tableColumnsDict[self.tableNames[0]].keys():
                                                                self.tableColumnsDict[self.tableNames[0]][i] = ""
                                            else:
                                                pass

                                    else:
                                        pass
                        try:
                            alias = column_dict['name']
                        except:
                            alias = ""
                        colltmp = {"base_col": final_col, "udf": udf, "Alias": alias, "table": self.tableNames[0]}
                        column_list.append(colltmp)
                        # print("final col:", final_col)
                        # if final_col not in self.tableColumnsDict[self.tableNames[0]].keys():
                        #     self.tableColumnsDict[self.tableNames[0]][column_value] = alias
        return column_list




    # =============================================================================
    #     FROM statement processing
    # =============================================================================

if __name__ == "__main__":
    from moz_sql_parser import parse
    import re

    query = """CREATE TABLE Currents AS  ( SELECT marsha  mars, stay_year, coalesce( crossover_rms,0) AS CO_RN_Goal, 
                coalesce( ( crossover_rms*crossover_gadr),0) AS CO_Rev_Goal, def_rms AS Def_OTB, 
                ( def_rms*def_gadr) AS Def_Rev, CID_Rms AS Target, Avg_rms AS Avg_Bkd 
                FROM AW_TGT_BUS_DBO.OY_ANNUALCROSSOVER 
                WHERE stay_year < &YEARNXT3 AND ASOF_YRMO=&CURYRPD; """
                # ORDER BY marsha, stay_year);"""

    query = """CREATE TABLE Futures AS  ( SELECT marsha mars, 'Futures' AS Stay_Year, 
                coalesce( SUM( B.crossover_rms),0) AS CO_RN_Goal, 
                coalesce( SUM( A.crossover_rms*B.def_gadr),0) AS CO_Rev_Goal, 
                SUM(def_rms) AS Def_OTB, SUM( def_rms*def_gadr) AS Def_Rev, 
                SUM( cid_rms) AS Target, SUM( avg_rms) AS Avg_Bkd 
                FROM AW_TGT_BUS_DBO.OY_ANNUALCROSSOVER A
                join table2 B on A.marsha = A.marsha
                WHERE stay_year > &curyr AND ASOF_YRMO=&CURYRPD 
                GROUP BY marsha ORDER BY marsha);"""

    # query = """CREATE TABLE merge_CrossOver1 AS ( SELECT * ,marsha AS marsha2,
    #             FORMAT(stay_year, VARCHAR(7)) AS stay_year , marsha2 AS marsha ,
    #             CASE WHEN co_rev_goal=' ' OR co_rev_goal=' ' OR co_rn_goal=0 OR co_rev_goal=0
    #             THEN 0 ELSE co_rev_goal/co_rn_goal END AS co_rn_goal_adr,
    #             CASE WHEN def_otb=' ' OR def_rev=' ' OR def_otb=' ' OR def_rev=0
    #             THEN 0 ELSE def_rev/def_otb END AS def_adr
    #             FROM Currents A JOIN Futures B ON A.marsha2 = B.marsha2 AND A.stay_year = B.stay_year);"""


    # query = """create table newTable  ( select a aaa, b as bbb,
    #            case when a = b then True when t1.m2 = t3.c then case when a = b then true else false end else False end as caseCol,
    #            t3.c t3c, t2.x2 as t2x2
    #            from table1 t1
    #            outer join table2 t2 on t1.colA = t2.colB
    #            and t1.col3 = t2.col4
    #            right join table3 t3 on t2.col = t3.c
    #                 and t4.col2 = t1.col5
    #          left join table4 t4 on t4.col2 = t1.col5
    #            where a=1 and (b != 2 and t2.x2 = True) or t3.c < 4
    #            group by b, t3.c
    #            order by t2.col2, a)
    #            """
    # query = """INSERT INTO  Currents ( SELECT marsha, stay_year, coalesce( crossover_rms,0) AS CO_RN_Goal,
    #         coalesce( ( crossover_rms*crossover_gadr),0) AS CO_Rev_Goal, def_rms AS Def_OTB, ( def_rms*def_gadr) AS Def_Rev,
    #         CID_Rms AS Target, Avg_rms AS Avg_Bkd FROM AW_TGT_BUS_DBO.OY_ANNUALCROSSOVER
    #         WHERE stay_year < YEARNXT3 AND ASOF_YRMO=CURYRPD ORDER BY marsha, stay_year);"""

    # query = """select  marsha, stay_year, def_rms as def_otb,
    #         cid_rms as target, avg_rms as avg_bkd from aw_tgt_bus_dbo.oy_annualcrossover
    #         where stay_year < yearnxt3 and asof_yrmo=curyrpd order by marsha, stay_year"""

    # query =  """select a.col1, a.*, a.col2, * from table1 a join table2 b on b.col1 = b.col2"""

    # print(parse(query))

    query = """create table newTable SELECT sum(B.alpha) as alpha1,
        coalesce( A.crossover_rms,0) as CO_RN_Goal,
        coalesce( ( A.crossover_rms*B.crossover_gadr),0) as CO_Rev_Goal,
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
        and A.Target in (1,2,3,4)
        and A.Avg_Bkd="ABCD"
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

    a = SQL_Pandas_Parser(query)
    # a.identifyTables()
    # a.identifyColumns()
    # print(a.tableColumnsDict)
    # print("query: ",a.sqlQuery)
    # print(parse(a.sqlQuery))
    # a.identifyTables()
    # a.identifyColumns()
    # print(a.handleWhereClauses())
    fileName = "query.py"
    f =  open(fileName, 'w')
    f.write('"""Query: ' + query + '"""\n\n')
    for s in a.buildPandasScript():
        f.write(s)
        f.write("\n")
    f.close()
