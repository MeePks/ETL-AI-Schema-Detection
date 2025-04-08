import pandas as pd
import sqlalchemy as sqla
import os

#create a connection to the database
def create_connection(database, server):
    try:
        connection_string = f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
        engine = sqla.create_engine(connection_string)
        print(f'Connection successful : {database} on {server}')
        return engine
    except Exception as e:
        print(f'Connection failed: {e}')
        return None

def create_database(engine,database_name,tier_name,folder_name,ndf_file_path):
    if not os.path.exists(ndf_file_path):
        print(f"Creating directory: {ndf_file_path}")
        try:
            os.makedirs(ndf_file_path)
        except Exception as e:
            print(f"Failed to create directory: {e}")
            return
    database_creation_query = fr'''
    CREATE DATABASE {database_name} CONTAINMENT = NONE ON  PRIMARY 
    ( NAME = N'AmazonDataInventory_Load001', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load001.mdf' , SIZE =102400KB , MAXSIZE = 524288000KB , FILEGROWTH = 5120000KB ),
    ( NAME = N'AmazonDataInventory_Load002', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load002.ndf' , SIZE = 10240KB , MAXSIZE = 524288000KB , FILEGROWTH = 5120000KB ),
    ( NAME = N'AmazonDataInventory_Load003', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load003.ndf' , SIZE = 10240KB , MAXSIZE = 524288000KB , FILEGROWTH = 5120000KB ),
    ( NAME = N'AmazonDataInventory_Load004', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load004.ndf' , SIZE = 10240KB , MAXSIZE = 524288000KB , FILEGROWTH = 5120000KB ),
    ( NAME = N'AmazonDataInventory_Load005', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load005.ndf' , SIZE = 10240KB , MAXSIZE = 524288000KB , FILEGROWTH = 5120000KB ),
    ( NAME = N'AmazonDataInventory_Load006', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load006.ndf' , SIZE = 10240KB , MAXSIZE = 524288000KB , FILEGROWTH = 5120000KB ),
    ( NAME = N'AmazonDataInventory_Load007', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load007.ndf' , SIZE = 10240KB , MAXSIZE = 524288000KB , FILEGROWTH = 5120000KB ),
    ( NAME = N'AmazonDataInventory_Load008', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load008.ndf' , SIZE = 10240KB , MAXSIZE = 524288000KB , FILEGROWTH = 5120000KB ),
    ( NAME = N'AmazonDataInventory_Load009', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load009.ndf' , SIZE = 10240KB , MAXSIZE = 524288000KB , FILEGROWTH = 5120000KB ),
    ( NAME = N'AmazonDataInventory_Load010', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load010.ndf' , SIZE = 10240KB , MAXSIZE = 524288000KB , FILEGROWTH = 5120000KB ),
    ( NAME = N'AmazonDataInventory_Load011', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load011.ndf' , SIZE = 10240KB , MAXSIZE = 524288000KB , FILEGROWTH = 5120000KB ),
    ( NAME = N'AmazonDataInventory_Load012', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load012.ndf' , SIZE = 10240KB , MAXSIZE = 524288000KB , FILEGROWTH = 5120000KB ),
    ( NAME = N'AmazonDataInventory_Load013', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load013.ndf' , SIZE = 10240KB , MAXSIZE = 524288000KB , FILEGROWTH = 5120000KB ),
    ( NAME = N'AmazonDataInventory_Load014', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load014.ndf' , SIZE = 10240KB , MAXSIZE = 524288000KB , FILEGROWTH = 5120000KB ),
    ( NAME = N'AmazonDataInventory_Load015', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load015.ndf' , SIZE = 10240KB , MAXSIZE = 524288000KB , FILEGROWTH = 5120000KB ),
    ( NAME = N'AmazonDataInventory_Load016', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load016.ndf' , SIZE = 10240KB , MAXSIZE = 524288000KB , FILEGROWTH = 5120000KB ), 
    FILEGROUP [IDX_EU] 
    ( NAME = N'AmazonDataInventory_Load_IDX_EU_001', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load_IDX_EU_001.ndf' , SIZE = 1024KB , MAXSIZE = UNLIMITED, FILEGROWTH = 5242880KB ),
    ( NAME = N'AmazonDataInventory_Load_IDX_EU_002', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load_IDX_EU_002.ndf' , SIZE = 1024KB , MAXSIZE = UNLIMITED, FILEGROWTH = 5242880KB ),
    ( NAME = N'AmazonDataInventory_Load_IDX_EU_003', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load_IDX_EU_003.ndf' , SIZE = 1024KB , MAXSIZE = UNLIMITED, FILEGROWTH = 5242880KB ),
    ( NAME = N'AmazonDataInventory_Load_IDX_EU_004', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load_IDX_EU_004.ndf' , SIZE = 1024KB , MAXSIZE = UNLIMITED, FILEGROWTH = 5242880KB ),
    ( NAME = N'AmazonDataInventory_Load_IDX_EU_005', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load_IDX_EU_005.ndf' , SIZE = 1024KB , MAXSIZE = UNLIMITED, FILEGROWTH = 5242880KB ),
    ( NAME = N'AmazonDataInventory_Load_IDX_EU_006', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load_IDX_EU_006.ndf' , SIZE = 1024KB , MAXSIZE = UNLIMITED, FILEGROWTH = 5242880KB ),
    ( NAME = N'AmazonDataInventory_Load_IDX_EU_007', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load_IDX_EU_007.ndf' , SIZE = 1024KB , MAXSIZE = UNLIMITED, FILEGROWTH = 5242880KB ),
    ( NAME = N'AmazonDataInventory_Load_IDX_EU_008', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load_IDX_EU_008.ndf' , SIZE = 1024KB , MAXSIZE = UNLIMITED, FILEGROWTH = 5242880KB ), 
    FILEGROUP [IDX_NA] 
    ( NAME = N'AmazonDataInventory_Load_IDX_NA_001', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load_IDX_NA_001.ndf' , SIZE = 1024KB , MAXSIZE = UNLIMITED, FILEGROWTH = 5242880KB ),
    ( NAME = N'AmazonDataInventory_Load_IDX_NA_002', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load_IDX_NA_002.ndf' , SIZE = 1024KB , MAXSIZE = UNLIMITED, FILEGROWTH = 5242880KB ),
    ( NAME = N'AmazonDataInventory_Load_IDX_NA_003', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load_IDX_NA_003.ndf' , SIZE = 1024KB , MAXSIZE = UNLIMITED, FILEGROWTH = 5242880KB ),
    ( NAME = N'AmazonDataInventory_Load_IDX_NA_004', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load_IDX_NA_004.ndf' , SIZE = 1024KB , MAXSIZE = UNLIMITED, FILEGROWTH = 5242880KB ),
    ( NAME = N'AmazonDataInventory_Load_IDX_NA_005', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load_IDX_NA_005.ndf' , SIZE = 1024KB , MAXSIZE = UNLIMITED, FILEGROWTH = 5242880KB ),
    ( NAME = N'AmazonDataInventory_Load_IDX_NA_006', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load_IDX_NA_006.ndf' , SIZE = 1024KB , MAXSIZE = UNLIMITED, FILEGROWTH = 5242880KB ),
    ( NAME = N'AmazonDataInventory_Load_IDX_NA_007', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load_IDX_NA_007.ndf' , SIZE = 1024KB , MAXSIZE = UNLIMITED, FILEGROWTH = 5242880KB ),
    ( NAME = N'AmazonDataInventory_Load_IDX_NA_008', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Load_IDX_NA_008.ndf' , SIZE = 1024KB , MAXSIZE = UNLIMITED, FILEGROWTH = 5242880KB )
    LOG ON 
    ( NAME = N'AmazonDataInventory_Loadlog01', FILENAME = N'D:\SQLData_AFS\{tier_name}AmazonDataInventory_Load\{folder_name}\AmazonDataInventory_Loadlog01.ldf' , SIZE = 1024KB , MAXSIZE = 2048GB , FILEGROWTH = 102400KB )
    GO

    IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
    begin
    EXEC {database_name}.[dbo].[sp_fulltext_database] @action = 'enable'
    end
    GO

    ALTER DATABASE {database_name} SET ANSI_NULL_DEFAULT OFF 
    GO

    ALTER DATABASE {database_name} SET ANSI_NULLS OFF 
    GO

    ALTER DATABASE {database_name} SET ANSI_PADDING OFF 
    GO

    ALTER DATABASE {database_name} SET ANSI_WARNINGS OFF 
    GO

    ALTER DATABASE {database_name} SET ARITHABORT OFF 
    GO

    ALTER DATABASE {database_name} SET AUTO_CLOSE OFF 
    GO

    ALTER DATABASE {database_name} SET AUTO_SHRINK OFF 
    GO

    ALTER DATABASE {database_name} SET AUTO_UPDATE_STATISTICS ON 
    GO

    ALTER DATABASE {database_name} SET CURSOR_CLOSE_ON_COMMIT OFF 
    GO

    ALTER DATABASE {database_name} SET CURSOR_DEFAULT  GLOBAL 
    GO

    ALTER DATABASE {database_name} SET CONCAT_NULL_YIELDS_NULL OFF 
    GO

    ALTER DATABASE {database_name} SET NUMERIC_ROUNDABORT OFF 
    GO

    ALTER DATABASE {database_name} SET QUOTED_IDENTIFIER OFF 
    GO

    ALTER DATABASE {database_name} SET RECURSIVE_TRIGGERS OFF 
    GO

    ALTER DATABASE {database_name} SET  DISABLE_BROKER 
    GO

    ALTER DATABASE {database_name} SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
    GO

    ALTER DATABASE {database_name} SET DATE_CORRELATION_OPTIMIZATION OFF 
    GO

    ALTER DATABASE {database_name} SET ALLOW_SNAPSHOT_ISOLATION OFF 
    GO

    ALTER DATABASE {database_name} SET PARAMETERIZATION SIMPLE 
    GO

    ALTER DATABASE {database_name} SET READ_COMMITTED_SNAPSHOT OFF 
    GO

    ALTER DATABASE {database_name} SET RECOVERY SIMPLE 
    GO

    ALTER DATABASE {database_name} SET  MULTI_USER 
    GO

    ALTER DATABASE {database_name} SET PAGE_VERIFY CHECKSUM  
    GO

    ALTER DATABASE {database_name} SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
    GO

    ALTER DATABASE {database_name} SET TARGET_RECOVERY_TIME = 60 SECONDS 
    GO

    ALTER DATABASE {database_name} SET DELAYED_DURABILITY = DISABLED 
    GO

    ALTER DATABASE {database_name} SET QUERY_STORE = OFF
    GO
    '''
    scripts=database_creation_query.split('GO')
    try:
        with engine.connect() as connection:
            for script in scripts:
                connection.execute(sqla.text(script))
                connection.commit()
        print(f"Database {database_name} created successfully")
    except Exception as e:
        print(f"Database creation failed due to: {e}")

#path refers to the folder containing the zip files
path=fr'Y:\Raw\Retail\Amazon\Inventory\2025-03-24'
database_name = 'AmazonDataInventory_202501'  #name of the database to be created

#configuring necessary parameters for database creation based on input from user
#user must select the tier in which database is to be created based on the row number of the table displayed
engine = create_connection('master', 'amazon.etl.sql.ccaintranet.com')
print('Checking Tier Space : Please wait for a while')
table=pd.read_sql_query(' select * from amazon.dbo._vw_CheckTierSpace order by FreeSpaceTB desc',engine)
print(table.to_string(index=False))
row_number = int(input("Enter the row number( select the tier in which DB is to be Created): "))
selected_row = table.iloc[row_number-1]
tier_name = selected_row['MountPoint']
folder_name = database_name.split('_')[-1]
ndf_file_path = r'Y:\SQLData\DC-CARBON\\' + selected_row['MountPoint'] + rf'AmazonDataInventory_Load\{folder_name}'
output_path=path.replace('Raw','Split')

#creating database
create_database(engine,database_name,tier_name,folder_name,ndf_file_path)





