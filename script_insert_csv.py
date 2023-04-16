import pandas as pd
import psycopg2
import decimal

# Ler dados do Excel
df_payroll = pd.read_csv("C:/Users/annea/Downloads/desafio1/arquivo_csv/nbaPayroll.csv")
df_salaries = pd.read_csv("C:/Users/annea/Downloads/desafio1/arquivo_csv/nbaSalaries.csv")
df_player_stats = pd.read_csv("C:/Users/annea/Downloads/desafio1/arquivo_csv/nbaPlayerStats.csv")
df_player_box = pd.read_csv("C:/Users/annea/Downloads/desafio1/arquivo_csv/nbaPlayerBoxScore.csv")


# Conectar-se ao banco de dados
conn = psycopg2.connect(
    host="localhost",
    database="bootcamp",
    user="root",
    password="root"
)

# Criar tabela no banco de dados
cur = conn.cursor()
cur.execute("""
    CREATE SCHEMA nba;

    CREATE TABLE nba.nbaPayroll(
        id serial,
        team varchar(100),
        seasonStartYear int,
        payroll varchar(20),
        inflationAdjPayroll varchar(30)
        );    

    CREATE TABLE nba.nbaSalaries(
        id serial,
        playerName varchar(100),
        seasonStartYear int,
        salary varchar(30),
        inflationAdjSalary varchar(30)
    );

    CREATE TABLE nba.nbaPlayerStats(
        id serial,
        Unnamed int,
        Season int,
        Player varchar(100),
        Pos varchar (30),
        Age int,
        Tm varchar(30),
        G float,
        GS float,
        MP float,
        FG_UM float,
        FGA decimal,
        FG_DOIS decimal,
        TRESP_UM float,
        TRESPA float,
        TRESP_DOIS decimal,
        DOISP_UM float,
        DOISPA float,
        DOISP_DOIS decimal,
        eFG decimal,
        FT_UM float,
        FTA float,
        FT_DOIS decimal,
        ORB float,
        DRB float,
        TRB float,
        AST float,
        STL float,
        BLK float,
        TOV float,
        PF float,
        PTS float);

    CREATE TABLE nba.nbaPlayerBox(
        id serial,
        Season int,
        Game_ID int,
        PLAYER_NAME	varchar(50),
        Team varchar(3),
        GAME_DATE varchar(15),
        MATCHUP	varchar(20),
        WL varchar(2),
        MIN int,
        FGM int,
        FGA float,
        FG_PCT decimal,
        FG3M float,
        FG3A float,
        FG3_PCT decimal,
        FTM int,
        FTA float,
        FT_PCT decimal,
        OREB float,
        DREB float,
        REB float,
        AST float,
        STL float,
        BLK	float,
        TOV	float, 
        PF float,
        PTS int,
        PLUS_MINUS decimal,
        VIDEO_AVAILABLE int);
""")    

# Inserir dados na tabela Payroll
data_payroll = [tuple(x) for x in df_payroll.to_numpy()]
cols = ','.join(list(df_payroll.columns))

query_payroll = f"INSERT INTO nba.nbaPayroll ({cols}) VALUES (%s, %s, %s, %s, %s)"
cur.executemany(query_payroll, data_payroll)

# Inserir dados na tabela Salaries
data_salaries = [tuple(x) for x in df_salaries.to_numpy()]
cols = ','.join(list(df_salaries.columns))

query_salaries = f"INSERT INTO nba.nbaSalaries ({cols}) VALUES (%s, %s, %s, %s, %s)"
cur.executemany(query_salaries, data_salaries)

# Inserir dados na tabela Player Stats
data_player_stats = [tuple(x) for x in df_player_stats.to_numpy()]
cols = ','.join(list(df_player_stats.columns))

query_player_stats = f"INSERT INTO nba.nbaPlayerStats ({cols}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
cur.executemany(query_player_stats, data_player_stats)

# Inserir dados na tabela Player Box
data_player_box = [tuple(x) for x in df_player_box.to_numpy()]
cols = ','.join(list(df_player_box.columns))

query_player_box = f"INSERT INTO nba.nbaPlayerBox ({cols}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
cur.executemany(query_player_box, data_player_box)

# Fechar conexão com o banco de dados
conn.commit()
cur.close()
conn.close()