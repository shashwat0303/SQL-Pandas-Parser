
# Author: Shashwat Koranne

OPERATORS = ['eq', 'lt', 'gt', 'lte', 'gte']

def addColumnToTable(columnName, tableColumsDict, tableAliases, baseTable, columnAlias=""):
    if "." in columnName:
        splits = columnName.split(".")
        tableAlias = splits[0]
        columnName = splits[1]
        tableName = tableAliases[tableAlias]
        columnDict = {columnName: columnAlias}
    else:
        tableName = baseTable
        columnDict = {columnName: columnAlias}
    if tableName in tableColumsDict.keys():
        if columnName not in tableColumsDict[tableName].keys():
            tableColumsDict[tableName][columnName] = columnAlias
    else:
        tableColumsDict[tableName] = columnDict

def exploreDict(dataStructure, tableColumsDict, tableAliases, baseTable, columnAlias):
    if type(dataStructure) == str:
        addColumnToTable(dataStructure, tableColumsDict, tableAliases, baseTable, columnAlias)
        return
    elif type(dataStructure) == list:
        return trace(dataStructure, 0, tableColumsDict, tableAliases, baseTable, columnAlias)
    elif type(dataStructure) == dict:
        # keys = list(dataStructure.keys())
        if 'then' in dataStructure.keys():
            if type(dataStructure['then']) != dict:
                del dataStructure['then']
        key = list(dataStructure.keys())[0]
        if type(dataStructure[key]) == str:
            newDataStructure = str(list(dataStructure.values())[0])
        else:
            newDataStructure = list(*dataStructure.values())
        if "case" in dataStructure.keys():
            newDataStructure = newDataStructure[:-1]
        return exploreDict(newDataStructure, tableColumsDict, tableAliases, baseTable, columnAlias)

def trace(colList, index, tableColumsDict, tableAliases, baseTable, columnAlias):
    if index >= len(colList):
        return
    if type(colList[index]) == list:
        trace(colList[index], index, tableColumsDict, tableAliases, baseTable, columnAlias)
    elif type(colList[index]) != dict:
        if type(colList[index]) != int:
            addColumnToTable(colList[index], tableColumsDict, tableAliases, baseTable, columnAlias)
    else:
        if 'then' in colList[index].keys():
            if type(colList[index]['then']) != dict:
                del colList[index]['then']
        dataStructure = list(colList[index].values())
        if len(dataStructure) == 1:
            dataStructure = dataStructure[0]
        if "case" in colList[index].keys():
            dataStructure = dataStructure[:-1]
        exploreDict(dataStructure, tableColumsDict, tableAliases, baseTable, columnAlias)
    index = index + 1
    trace(colList, index, tableColumsDict, tableAliases, baseTable, columnAlias)


def cleanColumnName(columName, tablesAliases, tableColumnsDict, tableNames):
    if "." in columName:
        splits = columName.split(".")
        tableAlias = splits[0]
        columName = splits[1]
        tableName = tablesAliases[tableAlias]
        columnAlias = tableColumnsDict[tableName][columName]
    else:
        tableName = tableNames[0]
        columnAlias = tableColumnsDict[tableName][columName]
    if columnAlias != "":
        return columnAlias, tableNames.index(tableName)
    else:
        return columName, tableNames.index(tableName)


def joinStatement(dataStructure, listOfCols):
    if type(dataStructure) == dict:
        if len(dataStructure.keys()) == 1 and list(dataStructure.keys())[0] in OPERATORS:
            operator = list(dataStructure.keys())[0]
            columns = dataStructure[operator]
            listOfCols.append(columns)
        else:
            joinStatement(list(dataStructure.values())[0], listOfCols)
    elif type(dataStructure) == list:
        listIter(dataStructure, listOfCols, 0)
    return listOfCols


def listIter(listToIter, listOfCols, index):
    if index >= len(listToIter):
        return
    else:
        if type(listToIter[index]) == dict:
            joinStatement(listToIter[index], listOfCols)
        else:
            listOfCols.append(listToIter[index])
        index = index + 1
        listIter(listToIter, listOfCols, index)

def cleanTableName(tableName):
    if '.' in tableName:
        if '[' in tableName:
            return tableName.split("[")[-1].replace("]", "").replace(";", "")
        else:
            return tableName.split(".")[-1].replace(";", "")
    return tableName.replace(";", "")

def bracketStringIndex(sql, start):
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

def renameColName(columnList, colName, tableColumnsDict, tableNames, tablesAliases, code = ""):
    if colName == "":
        return '""'
    if type(colName) == str and "." not in colName:
        if colName in columnList:
            updatedColumn = tableColumnsDict[tableNames[0]][colName]
            if updatedColumn != "":
                if code == "case":
                    return tableNames[0] + "[" + updatedColumn + "]"
                return updatedColumn
            else:
                if code == "case":
                    return tableNames[0] + "[" + colName + "]"
                return colName
        else:
            return '"' + colName + '"'
    elif type(colName) == str and "." in colName:
        colName, tableNum = cleanColumnName(colName, tablesAliases, tableColumnsDict, tableNames)
        if code == "case":
            return tableNames[0] + "[" + colName + "]"
    elif type(colName) == list:
        return str(tuple(colName))
    return colName

def performAction(operator, lhs, rhs, columnList, tableColumnsDict, tableNames, tablesAliases):
    lhs = renameColName(columnList, lhs, tableColumnsDict, tableNames, tablesAliases)
    rhs = renameColName(columnList, rhs, tableColumnsDict, tableNames, tablesAliases)
    if type(lhs) != str and type(lhs) != int: lhs = "'" + str(lhs) + "'"
    if type(rhs) != str and type(rhs) != int: rhs = "'" + str(rhs) + "'"
    if operator == "in":
        return str(lhs) + " in " + str(rhs)
    elif operator == "eq":
        return str(lhs) + " == " + str(rhs)
    return

def handleOperator(operator, conditionList):
    script = ""
    if conditionList != None:
        if operator == "and":
            for condition in conditionList:
                script += condition
                if condition != conditionList[-1]:
                    script += " & "
            return script
        elif operator == "or":
            for condition in conditionList:
                script += condition
                if condition != conditionList[-1]:
                    script += " | "
            return script
    return []

def handleWhereClause(dataStructure, columnList, tableColumnsDict, tableNames, tablesAliases):
    if type(dataStructure) == dict:
        key = list(dataStructure.keys())[0]
        value = dataStructure[key]
        if type(value[0]) == str:
            lhs = value[0]
            rhs = value[1]
            script = performAction(key, lhs, rhs, columnList, tableColumnsDict, tableNames, tablesAliases)
            return script
        else:
            whereConditions = whereList(value, 0, [], columnList, tableColumnsDict, tableNames, tablesAliases)
            script = handleOperator(key, whereConditions)
    else:
        script = whereList(dataStructure, 0, [], columnList, tableColumnsDict, tableNames, tablesAliases)

    return script

def whereList(listData, index, scriptList, columnList, tableColumnsDict, tableNames, tablesAliases):
    if index >= len(listData):
        return
    else:
        scriptList.append(handleWhereClause(listData[index], columnList, tableColumnsDict, tableNames, tablesAliases))
        index = index + 1
        whereList(listData, index, scriptList, columnList, tableColumnsDict, tableNames, tablesAliases)
    return scriptList


def udfScript(operator, colList, columnList, columnAlias, tableNames, tableAliases, tableColumnsDict):
    updatedColList = []
    baseTable = tableNames[0]
    script = ""
    if operator == "mul":
        for col in colList:
            updatedColumn = renameColName(columnList, col, tableColumnsDict, tableNames, tableAliases, code = "case")
            updatedColList.append(updatedColumn)
        product = " * ".join(updatedColList)
        script = baseTable + "['" + columnAlias + "'] = " + product
    elif operator == "coalesce":
        updatedColumn = renameColName(columnList, colList[0], tableColumnsDict, tableNames, tableAliases)
        script = baseTable + "['" + columnAlias + "'] = " + baseTable + "['" + updatedColumn + "'].fillna(value = " + str(colList[1]) + ")"
    elif operator == "sum":
        updatedColumn = renameColName(columnList, colList, tableColumnsDict, tableNames, tableAliases)
        script = baseTable + "['" + columnAlias + "'] = " + baseTable + "['" + updatedColumn + "'].sum()"
    return script


def handleUDFs(dataStructure, columnList, columnAlias, tableNames, tableAliases, tableColumnsDict, udfList):
    if type(dataStructure) == dict:
        updatedDataStructure = list(dataStructure.values())[0]
        if (type(updatedDataStructure) == list and type(updatedDataStructure[0]) != dict) or type(updatedDataStructure) == str:
            if dataStructure not in udfList: udfList.append(dataStructure)
        elif type(updatedDataStructure) == list and type(updatedDataStructure[0]) == dict:
            key = list(dataStructure.keys())[0]
            udfList, dataStructure = udfIter(updatedDataStructure, columnList, columnAlias, tableNames, tableAliases, tableColumnsDict, udfList, 0)
            updatedDataStructure = {key : dataStructure}
            handleUDFs(updatedDataStructure, columnList, columnAlias, tableNames, tableAliases, tableColumnsDict,
                       udfList)
        elif type(updatedDataStructure) == dict:
            handleUDFs(updatedDataStructure, columnList, columnAlias, tableNames, tableAliases, tableColumnsDict, udfList)
    elif type(dataStructure) == list:
        udfList, dataStructure = udfIter(dataStructure, columnList, columnAlias, tableNames, tableAliases, tableColumnsDict, udfList, 0)
    return udfList


def udfIter(dataStructure, columnList, columnAlias, tableNames, tableAliases, tableColumnsDict, udfList, index):
    if index >= len(dataStructure):
        return
    else:
        handleUDFs(dataStructure[index], columnList, columnAlias, tableNames, tableAliases, tableColumnsDict, udfList)
        if dataStructure[index] in udfList:
            dataStructure[index] = columnAlias
        index = index + 1
        udfIter(dataStructure, columnList, columnAlias, tableNames, tableAliases, tableColumnsDict, udfList, index)
    return udfList, dataStructure
