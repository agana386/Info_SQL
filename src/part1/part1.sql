-- part1 - cоздание таблиц

-- DROP TABLE IF EXISTS 
-- Peers,
-- Tasks,
-- P2P,
-- Verter,
-- Checks,
-- TransferredPoints,
-- Friends,
-- Recommendations,
-- XP,
-- TimeTracking;

-- DROP TYPE IF EXISTS CHECK_STATUS;

CREATE TABLE Peers (
    Nickname VARCHAR PRIMARY KEY,
    Birthday DATE
);

CREATE TABLE Tasks (
    Title VARCHAR PRIMARY KEY,
    ParentTask VARCHAR,
    MaxXP INT
);

CREATE TABLE Checks (
    ID serial PRIMARY KEY,
    Peer VARCHAR,
    Task VARCHAR,
    Date DATE,
	
    CONSTRAINT fk_Checks_Peer FOREIGN KEY (Peer) REFERENCES Peers(Nickname),
	CONSTRAINT fk_Checks_Task FOREIGN KEY (Task) REFERENCES Tasks(Title)
);

CREATE TYPE CHECK_STATUS AS ENUM ('Start', 'Success', 'Failure');

CREATE TABLE P2P (
    ID serial PRIMARY KEY,
    "check" BIGINT,
    CheckingPeer VARCHAR,
    State CHECK_STATUS,
    Time TIME,

    CONSTRAINT fk_P2P_Check FOREIGN KEY ("check") REFERENCES Checks(ID),
    CONSTRAINT fk_P2P_CheckingPeer FOREIGN KEY (CheckingPeer) REFERENCES Peers(Nickname)
);

CREATE TABLE Verter (
    ID serial PRIMARY KEY,
    "check" BIGINT,
    State CHECK_STATUS,
    Time TIME,

    CONSTRAINT fk_Verter_Check FOREIGN KEY ("check") REFERENCES Checks(ID)
);

CREATE TABLE TransferredPoints (
    ID serial PRIMARY KEY,
    CheckingPeer VARCHAR,
    CheckedPeer VARCHAR,
    PointsAmount INT,

    CONSTRAINT fk_TransferredPoints_CheckingPeer FOREIGN KEY (CheckingPeer) REFERENCES Peers(Nickname),
    CONSTRAINT fk_TransferredPoints_CheckedPeer FOREIGN KEY (CheckedPeer) REFERENCES Peers(Nickname)
);

CREATE TABLE Friends (
    ID serial PRIMARY KEY,
    Peer1 VARCHAR,
    Peer2 VARCHAR,

    CONSTRAINT fk_Friends_Peer1 FOREIGN KEY (Peer1) REFERENCES Peers(Nickname),
    CONSTRAINT fk_Friends_Peer2 FOREIGN KEY (Peer2) REFERENCES Peers(Nickname)
);

CREATE TABLE Recommendations (
  ID serial PRIMARY KEY,
  Peer VARCHAR,
  RecommendedPeer VARCHAR,

  CONSTRAINT fk_Recommendations_Peer FOREIGN KEY (Peer) REFERENCES Peers(Nickname),
  CONSTRAINT fk_Recommendations_RecommendedPeer FOREIGN KEY (RecommendedPeer) REFERENCES Peers(Nickname)
);

CREATE TABLE XP (
    ID serial PRIMARY KEY,
    "check" BIGINT,
    XPAmount INT,

    CONSTRAINT fk_XP_Check FOREIGN KEY ("check") REFERENCES Checks(ID)
);

CREATE TABLE TimeTracking (
    ID serial PRIMARY KEY,
    Peer VARCHAR,
    Date DATE,
    Time TIME,
    State INT,

    CONSTRAINT fk_TimeTracking_Peer FOREIGN KEY (Peer) REFERENCES Peers(Nickname),
	CONSTRAINT check_state CHECK (State = 1 OR State = 2)
);


-- Процедуры импорта и экспорта CSV
CREATE OR REPLACE PROCEDURE import_csv(
        IN table_name text,
        IN csv_file text,
        IN delimiter char DEFAULT ';'
    ) AS $$
DECLARE data_path text := 'C:\s21projects\csv\';
BEGIN EXECUTE format(
    'COPY %s FROM ''%s'' DELIMITER ''%s'' CSV HEADER NULL AS ''null'';',
    table_name,
    data_path || csv_file,
    delimiter
);
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE PROCEDURE export_csv(
        IN table_name text,
        IN csv_file text,
        IN delimiter char DEFAULT ';'
    ) AS $$
DECLARE data_path text := 'C:\s21projects\csv\';
BEGIN EXECUTE format(
    'COPY %s TO ''%s'' DELIMITER ''%s'' CSV HEADER NULL AS ''null'';',
    table_name,
    data_path || csv_file,
    delimiter
);
END;
$$ LANGUAGE plpgsql;
-- Создание базы данных
-- SET datestyle = dmy;
CALL import_csv('Peers', 'peers.csv', ';');
CALL import_csv('Tasks', 'tasks.csv', ';');
CALL import_csv('Checks', 'checks.csv', ';');
CALL import_csv('P2P', 'p2p.csv', ';');
CALL import_csv('Verter', 'verter.csv', ';');
CALL import_csv('TransferredPoints', 'transfer.csv', ';');
CALL import_csv('Friends', 'friends.csv', ';');
CALL import_csv('Recommendations', 'recommends.csv', ';');
CALL import_csv('XP', 'xp.csv', ';');
CALL import_csv('TimeTracking', 'timetrack.csv', ';');
--Вызов функции экспорта
CALL export_csv('Peers', 'peers.csv', ';');
CALL export_csv('Tasks', 'tasks.csv', ';');
CALL export_csv('Checks', 'checks.csv', ';');
CALL export_csv('P2P', 'p2p.csv', ';');
CALL export_csv('Verter', 'verter.csv', ';');
CALL export_csv('TransferredPoints', 'transfer.csv', ';');
CALL export_csv('Friends', 'friends.csv', ';');
CALL export_csv('Recommendations', 'recommends.csv', ';');
CALL export_csv('XP', 'xp.csv', ';');
CALL export_csv('TimeTracking', 'timetrack.csv', ';');
