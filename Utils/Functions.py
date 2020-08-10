
# Author: Shashwat Koranne

# This class defines methods to identify different
# statements from the given SQL query

# class Functions():

#     sqlQuery = ""
#     queryDict = {}
#     createTableAlias = ""
#     insertTableAlias = ""
#     createTable = False
#     insertTable = False
#
#     def __init__(self, sqlQuery):
#         self.sqlQuery = sqlQuery
#         self.cleanQuery()
#         self.queryDict = parse(self.sqlQuery)

# def cleanQuery(self):
#     # Remove extra spaces from the query
#     self.sqlQuery = re.sub("\s+", " ", self.sqlQuery)
#     if "outer join" in self.sqlQuery:
#         self.sqlQuery = self.sqlQuery.replace(" outer join ", " full outer join ")
#
#     # Remove create table clause from the query as moz sql parser cant handle it
#     if "create table" in self.sqlQuery:
#         regex = "CREATE TABLE (.*?)SELECT"
#         matches = re.finditer(regex, self.sqlQuery, re.IGNORECASE)
#         for matchNum, match in enumerate(matches, start=1):
#             createClause = match.group()
#             createClauseWOSelect = createClause.replace(" as ", " ").replace("(", "").split("select")[0]
#             splits = createClauseWOSelect.split()
#             self.createTableAlias = splits[-1]
#             self.createTable = True
#             self.sqlQuery = self.sqlQuery.replace(createClause, "")
#             if "(" == createClause.split()[-2]:
#                 self.sqlQuery = "(select " + self.sqlQuery
#                 closingBracketIndex = self.bracketStringIndex(self.sqlQuery, 0)
#                 self.sqlQuery = self.sqlQuery[1:closingBracketIndex].strip()
#             else:
#                 self.sqlQuery = "select " + self.sqlQuery.strip()
#
#     # Remove insert table clause from the query as moz sql parser cant handle it
#     if "insert into" in self.sqlQuery:
#         regex = "insert into(.*?)select"
#         matches = re.finditer(regex, self.sqlQuery, re.IGNORECASE)
#         for matchNum, match in enumerate(matches, start=1):
#             insertClause = match.group()
#             tempInsertClause = insertClause.replace("(", "").strip().split("select")[0]
#             splits = tempInsertClause.split()
#             self.insertTableAlias = self.cleanTableName(splits[-1])
#             self.insertTable = True
#             self.sqlQuery = self.sqlQuery.replace(insertClause, "")
#             if "(" == insertClause.split()[-2]:
#                 self.sqlQuery = "(select " + self.sqlQuery
#                 closingBracketIndex = self.bracketStringIndex(self.sqlQuery, 0)
#                 self.sqlQuery = self.sqlQuery[1:closingBracketIndex].strip()
#             else:
#                 self.sqlQuery = "select " + self.sqlQuery.strip()
#
#     # Remove '&' from the query as it shows up during SAS conversion as moz sql parser cant handle it
#     regex = "\&(.*?)[\s;]|[)]"
#     matches = re.finditer(regex, self.sqlQuery, re.IGNORECASE)
#     if matches is not None:
#         for matchNum, match in enumerate(matches, start=1):
#             word = match.group()
#             newWord = word.replace("&", "")
#             self.sqlQuery = self.sqlQuery.replace(word, newWord)
#     return

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

if __name__ == '__main__':
    from moz_sql_parser import parse
    import re