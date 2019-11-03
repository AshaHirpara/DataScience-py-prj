#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sqlite3
import pandas as pd
import csv


# In[2]:


pd.set_option('max_columns', 180)
pd.set_option('max_rows', 200000)
pd.set_option('max_colwidth', 5000)


# In[3]:


game = pd.read_csv('game_log.csv', low_memory = False)
print(game.shape)
print("\n")
game.head()


# In[4]:


game.tail()


# * More than 17000 game record
# * Information on the game such 
# * Information about team stats, number of team members, winning and loosing pitcher
# * Team player's positions
# * information about umpires of that game
# 
# **game_log_fields.txt** shows that there is no primary key column in given data file.
# 

# In[5]:


person = pd.read_csv('person_codes.csv')
print(person.shape)
person.head()


# In[6]:


person.tail()


# * List of people with IDs
# * The IDs used as foreign key in the game log
# * Debut dates, for players, managers, coaches and umpires
# * Some people might have one or more of these roles
# * Coaches and managers are two different things in baseball

# In[7]:


park = pd.read_csv('park_codes.csv')
print(park.shape)
park.head()


# In[8]:


park.tail()


# * List of all baseball parks and details start and end date of the game
# * Some of the field is used as foreign key for game_log data 
#     *  IDs,names, nicknames, city and league

# In[9]:


team = pd.read_csv('team_codes.csv')
print(team.shape)
team.head()


# * Team information
# * intresting filed "franch_id" probably for "franchise"
# * for the BFN and BFP, we can see that  teams move between leagues and cities
# 

# ### What each defensive position number represents?
# ----
# As mentioned in [this site](http://probaseballinsider.com/baseball-instruction/baseball-basics/baseball-basics-positions/) nuber suggest the following positions for:
#     1. Pitcher
#     2. Catcher
#     3. 1st Base
#     4. 2nd Base
#     5. 3rd Base
#     6. Shortstop
#     7. Left Field
#     8. Center Field
#     9. Right Field

# ### Values in the league fields and which leagues they represent?
# ------

# In[10]:


game["h_league"].unique()


# In[11]:


# league information fuction to get year data for each league

def league_info(league):
    league_game = game[game["h_league"]== league]
    earliest = league_game["date"].min()
    latest = league_game["date"].max()
    print("{} from {} to {}".format(league, earliest, latest))
    
for league in game["h_league"].unique():
    league_info(league)


# * NL: National League
# * AL: American League 
# * AA: American Association
# * FL: Federal League
# * PL: Players League
# * UA: Union Association

# ## Importing Data into SQLite

# In[12]:


DB = "mlb.db"

def run_query(q):
    with sqlite3.connect(DB) as conn:
        return pd.read_sql(q, conn)

def run_command(c):
    with sqlite3.connect(DB) as conn:
        conn.execute('PRAGMA foreign_keys = ON;')
        conn.isolation_level = None
        conn.execute(c)
        
def show_tables():
    q = '''
    select 
        name, type
    from 
        sqlite_master
    where
        type IN("table", "view");
    '''
    return run_query(q)


# In[13]:


tables = {
    "game_log": game,
    "person_codes": person,
    "team_codes": team,
    "park_codes": park
}

with sqlite3.connect(DB) as conn:
    for name,data in tables.items():
        conn.execute("DROP TABLE IF EXISTS {};".format(name))
        data.to_sql(name,conn,index=False)


# In[14]:


show_tables()


# In[15]:


# create a new column in the game_log table called game_id
c1 = """
ALTER TABLE game_log
ADD COLUMN game_id TEXT;
"""

# try/except loop since ALTER TABLE
# doesn't support IF NOT EXISTS

try:
    run_command(c1)
except:
    pass

# SQL string concatenation to update the new columns 
# with a unique ID using the Retrosheet format

c2 = """
UPDATE game_log
SET game_id = date || h_name || number_of_game
WHERE game_id IS NULL;
"""
run_command(c2)

q = """
SELECT
    game_id,
    date,
    h_name,
    number_of_game
FROM game_log
LIMIT 5;
"""

run_query(q)


# ## Normalized schema
# ------
# This schema is designed using [DbDesigner.net](https://www.dbdesigner.net/)
# ![mlb normalized schema](mlb.png)
# 
# ## Create tables without foreign keys
# 
# 

# In[17]:


# Create "person" table
#-----------
# step 1: Create table
# step 2: Insert values from original table
# step 3: run command and query
#------------

c1= """
create table if not exists person (
person_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT
);
"""
c2= """
insert or ignore into person
select id, first,last from person_codes;
"""
q = """select * from person limit 5;"""

run_command(c1)
run_command(c2)
run_query(q)


# In[18]:


# create "park" table


c1 = """
CREATE TABLE IF NOT EXISTS park (
    park_id TEXT PRIMARY KEY,
    name TEXT,
    nickname TEXT,
    city TEXT,
    state TEXT,
    notes TEXT
);
"""

c2 = """
INSERT OR IGNORE INTO park
SELECT
    park_id,
    name,
    aka,
    city,
    state,
    notes
FROM park_codes;
"""

q = """
SELECT * FROM park
LIMIT 5;
"""

run_command(c1)
run_command(c2)
run_query(q)


# In[19]:


# create "league" table

c1 = """
CREATE TABLE IF NOT EXISTS league (
    league_id TEXT PRIMARY KEY,
    name TEXT
);
"""

c2 = """
INSERT OR IGNORE INTO league
VALUES
    ("NL", "National League"),
    ("AL", "American League"),
    ("AA", "American Association"),
    ("FL", "Federal League"),
    ("PL", "Players League"),
    ("UA", "Union Association")
;
"""

q = """
SELECT * FROM league
"""

run_command(c1)
run_command(c2)
run_query(q)


# In[20]:


c1 = "DROP TABLE IF EXISTS appearance_type;"

run_command(c1)

c2 = """
CREATE TABLE appearance_type (
    appearance_type_id TEXT PRIMARY KEY,
    name TEXT,
    category TEXT
);
"""
run_command(c2)

appearance_type = pd.read_csv('appearance_type.csv')

with sqlite3.connect('mlb.db') as conn:
    appearance_type.to_sql('appearance_type',
                           conn,
                           index=False,
                           if_exists='append')

q = """
SELECT * FROM appearance_type;
"""

run_query(q)


# In[21]:


# create "team" table

c1 = """
CREATE TABLE IF NOT EXISTS team (
    team_id TEXT PRIMARY KEY,
    league_id TEXT,
    city TEXT,
    nickname TEXT,
    franch_id TEXT,
    FOREIGN KEY (league_id) REFERENCES league(league_id)
);
"""

c2 = """
INSERT OR IGNORE INTO team
SELECT
    team_id,
    league,
    city,
    nickname,
    franch_id
FROM team_codes;
"""

q = """
SELECT * FROM team
LIMIT 5;
"""

run_command(c1)
run_command(c2)
run_query(q)


# In[22]:


# create "game" table

c1 = """
CREATE TABLE IF NOT EXISTS game (
    game_id TEXT PRIMARY KEY,
    date TEXT,
    number_of_game INTEGER,
    park_id TEXT,
    length_outs INTEGER,
    day BOOLEAN,
    completion TEXT,
    forefeit TEXT,
    protest TEXT,
    attendance INTEGER,
    legnth_minutes INTEGER,
    additional_info TEXT,
    acquisition_info TEXT,
    FOREIGN KEY (park_id) REFERENCES park(park_id)
);
"""
c2 = """
insert or ignore into game
select 
    game_id,
    date,
    number_of_game,
    park_id,
    length_outs,
    case 
        when day_night = "D" then 1
        when day_night = "N" then 0
        else NULL
        end AS day,
    completion,
    forefeit,
    protest,
    attendance,
    length_minutes,
    additional_info,
    acquisition_info
FROM game_log;
"""
q = """
SELECT * FROM game
LIMIT 5;
"""

run_command(c1)
run_command(c2)
run_query(q)


# In[23]:


# create "team_appearance" table
c1 = """
CREATE TABLE IF NOT EXISTS team_appearance (
    team_id TEXT,
    game_id TEXT,
    home BOOLEAN,
    league_id TEXT,
    score INTEGER,
    line_score TEXT,
    at_bats INTEGER,
    hits INTEGER,
    doubles INTEGER,
    triples INTEGER,
    homeruns INTEGER,
    rbi INTEGER,
    sacrifice_hits INTEGER,
    sacrifice_flies INTEGER,
    hit_by_pitch INTEGER,
    walks INTEGER,
    intentional_walks INTEGER,
    strikeouts INTEGER,
    stolen_bases INTEGER,
    caught_stealing INTEGER,
    grounded_into_double INTEGER,
    first_catcher_interference INTEGER,
    left_on_base INTEGER,
    pitchers_used INTEGER,
    individual_earned_runs INTEGER,
    team_earned_runs INTEGER,
    wild_pitches INTEGER,
    balks INTEGER,
    putouts INTEGER,
    assists INTEGER,
    errors INTEGER,
    passed_balls INTEGER,
    double_plays INTEGER,
    triple_plays INTEGER,
    PRIMARY KEY (team_id, game_id), 
    FOREIGN KEY (team_id) REFERENCES team(team_id),
    FOREIGN KEY (game_id) REFERENCES game(game_id),
    FOREIGN KEY (team_id) REFERENCES team(team_id)
); """

# above we created composite primary key with (team_id, game_id)

run_command(c1)

c2 = """
INSERT OR IGNORE INTO team_appearance
    SELECT
        h_name,
        game_id,
        1 AS home,
        h_league,
        h_score,
        h_line_score,
        h_at_bats,
        h_hits,
        h_doubles,
        h_triples,
        h_homeruns,
        h_rbi,
        h_sacrifice_hits,
        h_sacrifice_flies,
        h_hit_by_pitch,
        h_walks,
        h_intentional_walks,
        h_strikeouts,
        h_stolen_bases,
        h_caught_stealing,
        h_grounded_into_double,
        h_first_catcher_interference,
        h_left_on_base,
        h_pitchers_used,
        h_individual_earned_runs,
        h_team_earned_runs,
        h_wild_pitches,
        h_balks,
        h_putouts,
        h_assists,
        h_errors,
        h_passed_balls,
        h_double_plays,
        h_triple_plays
    FROM game_log

UNION

    SELECT    
        v_name,
        game_id,
        0 AS home,
        v_league,
        v_score,
        v_line_score,
        v_at_bats,
        v_hits,
        v_doubles,
        v_triples,
        v_homeruns,
        v_rbi,
        v_sacrifice_hits,
        v_sacrifice_flies,
        v_hit_by_pitch,
        v_walks,
        v_intentional_walks,
        v_strikeouts,
        v_stolen_bases,
        v_caught_stealing,
        v_grounded_into_double,
        v_first_catcher_interference,
        v_left_on_base,
        v_pitchers_used,
        v_individual_earned_runs,
        v_team_earned_runs,
        v_wild_pitches,
        v_balks,
        v_putouts,
        v_assists,
        v_errors,
        v_passed_balls,
        v_double_plays,
        v_triple_plays
    from game_log;
"""
# home as 1 for home team
# home as 0 for visiting team

run_command(c2)

q = """
SELECT * FROM team_appearance
WHERE game_id = (
                 SELECT MIN(game_id) from game
                )
   OR game_id = (
                 SELECT MAX(game_id) from game
                )
ORDER By game_id, home;
"""

run_query(q)


# In[24]:


# create "person_appearance" table
c0 = "DROP TABLE IF EXISTS person_appearance"

run_command(c0)

c1 = """
CREATE TABLE person_appearance (
    appearance_id INTEGER PRIMARY KEY,
    person_id TEXT,
    team_id TEXT,
    game_id TEXT,
    appearance_type_id,
    FOREIGN KEY (person_id) REFERENCES person(person_id),
    FOREIGN KEY (team_id) REFERENCES team(team_id),
    FOREIGN KEY (game_id) REFERENCES game(game_id),
    FOREIGN KEY (appearance_type_id) REFERENCES appearance_type(appearance_type_id)
);
"""
# hp_umpire_id  with "UHP"
# 1b_umpire_id with "U1B"
# 2b_umpire_id with "U2B"
# 3b_umpire_id with "U3B"
# lf_umpire_id with "ULF"
# rf_umpire_id with "URF"
# if visiting team:team_id = v_name, person_id = v_manager_id, appearance_id ="MM"
# if home team:team_id = h_name, person_id = h_manager_id, appearance_id ="MM"

c2 = """
INSERT OR IGNORE INTO person_appearance (
    game_id,
    team_id,
    person_id,
    appearance_type_id
) 
    SELECT
        game_id,
        NULL,
        hp_umpire_id,
        "UHP"
    FROM game_log
    WHERE hp_umpire_id IS NOT NULL    

UNION

    SELECT
        game_id,
        NULL,
        [1b_umpire_id],
        "U1B"
    FROM game_log
    WHERE "1b_umpire_id" IS NOT NULL

UNION

    SELECT
        game_id,
        NULL,
        [2b_umpire_id],
        "U2B"
    FROM game_log
    WHERE [2b_umpire_id] IS NOT NULL

UNION

    SELECT
        game_id,
        NULL,
        [3b_umpire_id],
        "U3B"
    FROM game_log
    WHERE [3b_umpire_id] IS NOT NULL

UNION

    SELECT
        game_id,
        NULL,
        lf_umpire_id,
        "ULF"
    FROM game_log
    WHERE lf_umpire_id IS NOT NULL

UNION

    SELECT
        game_id,
        NULL,
        rf_umpire_id,
        "URF"
    FROM game_log
    WHERE rf_umpire_id IS NOT NULL

UNION

    SELECT
        game_id,
        v_name,
        v_manager_id,
        "MM"
    FROM game_log
    WHERE v_manager_id IS NOT NULL

UNION

    SELECT
        game_id,
        h_name,
        h_manager_id,
        "MM"
    FROM game_log
    WHERE h_manager_id IS NOT NULL

UNION

    SELECT
        game_id,
        CASE
            WHEN h_score > v_score THEN h_name
            ELSE v_name
            END,
        winning_pitcher_id,
        "AWP"
    FROM game_log
    WHERE winning_pitcher_id IS NOT NULL

UNION

    SELECT
        game_id,
        CASE
            WHEN h_score < v_score THEN h_name
            ELSE v_name
            END,
        losing_pitcher_id,
        "ALP"
    FROM game_log
    WHERE losing_pitcher_id IS NOT NULL

UNION

    SELECT
        game_id,
        CASE
            WHEN h_score > v_score THEN h_name
            ELSE v_name
            END,
        saving_pitcher_id,
        "ASP"
    FROM game_log
    WHERE saving_pitcher_id IS NOT NULL

UNION

    SELECT
        game_id,
        CASE
            WHEN h_score > v_score THEN h_name
            ELSE v_name
            END,
        winning_rbi_batter_id,
        "AWB"
    FROM game_log
    WHERE winning_rbi_batter_id IS NOT NULL

UNION

    SELECT
        game_id,
        v_name,
        v_starting_pitcher_id,
        "PSP"
    FROM game_log
    WHERE v_starting_pitcher_id IS NOT NULL

UNION

    SELECT
        game_id,
        h_name,
        h_starting_pitcher_id,
        "PSP"
    FROM game_log
    WHERE h_starting_pitcher_id IS NOT NULL;
"""

template = """
INSERT INTO person_appearance (
    game_id,
    team_id,
    person_id,
    appearance_type_id
) 
    SELECT
        game_id,
        {hv}_name,
        {hv}_player_{num}_id,
        "O{num}"
    FROM game_log
    WHERE {hv}_player_{num}_id IS NOT NULL

UNION

    SELECT
        game_id,
        {hv}_name,
        {hv}_player_{num}_id,
        "D" || CAST({hv}_player_{num}_def_pos AS INT)
    FROM game_log
    WHERE {hv}_player_{num}_id IS NOT NULL;
"""

run_command(c1)
run_command(c2)

for hv in ["h","v"]:
    for num in range(1,10):
        query_vars = {
            "hv": hv,
            "num": num
        }
        run_command(template.format(**query_vars))


# In[25]:


print(run_query("SELECT COUNT(DISTINCT game_id) games_game FROM game"))
print(run_query("SELECT COUNT(DISTINCT game_id) games_person_appearance FROM person_appearance"))

q = """
SELECT
    pa.*,
    at.name,
    at.category
FROM person_appearance pa
INNER JOIN appearance_type at on at.appearance_type_id = pa.appearance_type_id
WHERE PA.game_id = (
                   SELECT max(game_id)
                    FROM person_appearance
                   )
ORDER BY team_id, appearance_type_id
"""

run_query(q)


# ## Remove unnormalized data

# In[26]:


show_tables()


# In[27]:


tables = [
    "game_log",
    "park_codes",
    "team_codes",
    "person_codes"
]

for t in tables:
    c = '''
    DROP TABLE {}
    '''.format(t)
    
    run_command(c)

show_tables()


# In[ ]:




