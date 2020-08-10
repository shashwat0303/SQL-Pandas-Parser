def importPackage(impPkg, alias = "", frmPkg = ""):
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

def readPandasDFs(tableColumnsDict):
    finalScript = []
    tables = tableColumnsDict.keys()
    for table in tables:
        columns = list(tableColumnsDict[table].keys())
        sqlScript = "select " + ", ".join(columns) + " from " + table
        script = table + " = pd.read_sql(" + sqlScript + ")"
        finalScript.append(script)
    return finalScript

def renameColumns(tableColumnsDict):
    finalScript = []
    tables = tableColumnsDict.keys()
    for table in tables:
        columns = list(tableColumnsDict[table].keys())
        renameDict = {}
        for column in columns:
            columnAlias = tableColumnsDict[table][column]
            if columnAlias != "":
                renameDict[column] = columnAlias
        script = table + " = " + table + ".rename(columns = " + str(renameDict) + ")"
        finalScript.append(script)
    return finalScript

# def buildCaseScript(caseStatementsDict):
#     for caseStatement in caseStatementsDict:
#         caseCondition = caseCondition['case']

def handleCaseStatement(caseDict, conditions):
    if len(caseDict.keys()) == 1 and type(caseDict[list(caseDict.keys)[0]]) == list:
        operator = list(caseDict.keys)[0]
        columns = caseDict[operator]
    # else:



# def joinQuery(queryDict):
#     queryScript = []
#     baseTable = ""
#     from_dict = queryDict['from']
#     if type(from_dict) != list:
#         from_dict = [from_dict]
#     if len(from_dict) > 1:
#         for tableDetails in from_dict:
#             if "value" in tableDetails.keys():
#                 baseTable = tableDetails['value']
#             if "join" in tableDetails.keys():
#                 joinDetails = tableDetails['join']
#                 joinType = "'inner'"
#                 tableToMerge = joinDetails['value']
#                 joinConditions = tableDetails['on']
#                 script = self.handleJoinConditions(baseTable, tableToMerge, joinConditions, joinType)
#                 queryScript.append(script)
#             elif "left join" in tableDetails.keys():
#                 joinDetails = tableDetails['left join']
#                 joinType = "'left'"
#                 tableToMerge = joinDetails['value']
#                 joinConditions = tableDetails['on']
#                 script = self.handleJoinConditions(baseTable, tableToMerge, joinConditions, joinType)
#                 queryScript.append(script)
#             elif "right join" in tableDetails.keys():
#                 joinDetails = tableDetails['right join']
#                 joinType = "'right'"
#                 tableToMerge = joinDetails['value']
#                 joinConditions = tableDetails['on']
#                 script = self.handleJoinConditions(baseTable, tableToMerge, joinConditions, joinType)
#                 queryScript.append(script)
#             elif "full outer join" in tableDetails.keys():
#                 joinDetails = tableDetails['full outer join']
#                 joinType = "'outer'"
#                 tableToMerge = joinDetails['value']
#                 joinConditions = tableDetails['on']
#                 script = self.handleJoinConditions(baseTable, tableToMerge, joinConditions, joinType)
#                 queryScript.append(script)
#         removeDuplicates = baseTable + " = " + baseTable + ".loc[:, ~" + baseTable + ".columns.duplicated()]"
#         queryScript.append(removeDuplicates)
#     return queryScript