from Templates.PythonScript import PythonScript


class SQL_Pandas_Parser(PythonScript):

    def __init__(self, sqlQuery):
        super().__init__(sqlQuery)


if __name__ == '__main__':

    import pandas as pd
    import numpy as np
    sqlQueries = pd.read_csv("sqlqueries.csv")
    queries = np.array(sqlQueries['SQL'])
    fileNames = np.array(sqlQueries['filename'])

    for i in range(len(queries)):
        spp = SQL_Pandas_Parser(queries[i])
        scripts = spp.buildPandasScript()
        with open(fileNames[i] + ".py", 'a') as f:
            for script in scripts:
                f.write(script)
