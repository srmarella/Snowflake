import pyodbc
import pandas as pd
import os

# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port
server = 'pf-sql-qas'
database = 'dde'
username = 'srikanth.marella'
password = 'mypassword'
cnxn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database
    + ';Trusted_Connection=yes;MARS_Connection=Yes'
    )
cursor = cnxn.cursor()
cursor.execute('SELECT 1')

# # ------------------------------------------------------------------------------------ #
# # get all data                                                                         #
# # ------------------------------------------------------------------------------------ #
# chunk_size = 10
# offset = 0
# #dfs = []
# while True:
#     sql = "SELECT \
#         ParseDataId \
#         ,DecisionResultId \
#         ,AccountId \
#         ,BureauId \
#         ,PrimaryKey \
#         ,ParsedData \
#         ,CreatedDate \
#         ,CreatedByName \
#     FROM dbo.ParseData order by parsedataid OFFSET %d ROWS FETCH NEXT %d ROWS ONLY;" % (offset, chunk_size)

#     dfs = pd.read_sql(sql, cnxn)
#     offset += chunk_size
#     if len(dfs[-1]) < chunk_size:
#         break

# full_df = pd.concat(dfs)


# ------------------------------------------------------------------------------------ #
# get all data                                                                         #
# ------------------------------------------------------------------------------------ #
i: int = 0
j: int = 4

# file_dir = os.path.dirname(os.path.abspath(__file__)
path: str = r'C:\Users\srikanth.marella\Documents\GitHub\Snowflake\Semi-Structured'
outname = 'parsedata_json.csv'
outdir = 'parsedata_json'
path_dir: str = os.path.join(path, outdir)
chunk_size = 10_000
offset = 0

# run sql query and write to multiple files
while i <= j:
    sql = "SELECT \
        ParseDataId \
        ,DecisionResultId \
        ,AccountId \
        ,BureauId \
        ,PrimaryKey \
        ,ParsedData \
        ,CreatedDate \
        ,CreatedByName \
    FROM dbo.ParseData order by parsedataid OFFSET %d ROWS FETCH NEXT %d ROWS ONLY;" % (offset, chunk_size)

    dfs = pd.read_sql(sql, cnxn)
    offset += chunk_size
    fullname = os.path.join(path_dir + '\\' + str(i) + '_'+outname)

    if not os.path.exists(path_dir):
        os.mkdir(path_dir)

    dfs.to_csv(fullname, index=False)
    i = i+1
