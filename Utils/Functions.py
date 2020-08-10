
# Author: Shashwat Koranne

OPERATORS = ['eq', 'lt', 'gt', 'lte', 'gte']

def addColumnToTable(columnName, tableColumsDict, tableAliases, baseTable, columnAlias):
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
        splits = columName.aplit(".")
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










def exploreDict1(dataStructure, columnList):
    if type(dataStructure) == str:
        if dataStructure not in columnList: columnList.append(dataStructure)
        return
    elif type(dataStructure) == list:
        return trace(dataStructure, 0, columnList)
    elif type(dataStructure) == dict:
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
        return exploreDict(newDataStructure, columnList)

def trace1(colList, index, columnList):
    if index >= len(colList):
        return
    if type(colList[index]) == list:
        trace(colList[index], index, columnList)
    elif type(colList[index]) != dict:
        if type(colList[index]) != int:
            if colList[index] not in columnList: columnList.append(colList[index])
    else:
        if 'then' in colList[index].keys():
            if type(colList[index]['then']) != dict:
                del colList[index]['then']
        dataStructure = list(colList[index].values())
        if len(dataStructure) == 1:
            dataStructure = dataStructure[0]
        if "case" in colList[index].keys():
            dataStructure = dataStructure[:-1]
        exploreDict(dataStructure, columnList)
    index = index + 1
    trace(colList, index, columnList)

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

# if __name__ == '__main__':
    from moz_sql_parser import parse
    import re

d = {'or': [{'and': [{'eq': ['a.marsha', 'b.marsha']}, {'eq': ['a.marsha1', 'b.marsha1']}]}, {'lte': ['a.marsha2', 'b.marsha2']}]}

print(joinStatement(d, []))
