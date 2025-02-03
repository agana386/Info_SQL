-- CREATE DATABASE tempdb;

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

DROP FUNCTION IF EXISTS get_transferred_points_hr();
CREATE OR REPLACE FUNCTION get_transferred_points_hr()
RETURNS TABLE (Peer1 VARCHAR, Peer2 VARCHAR, PointsAmount INT) AS $$
BEGIN
	RETURN QUERY
	SELECT checkingpeer,
		   checkedpeer,
	CASE 
	WHEN ( SELECT temp2.pointsamount
		   FROM transferredpoints AS temp2
	       WHERE checkingpeer = temp1.checkedpeer
		   AND checkedpeer = temp1.checkingpeer
	     ) > temp1.pointsamount
	THEN 
		 temp1.pointsamount * -1
	ELSE
	 	 temp1.pointsamount
END
FROM transferredpoints AS temp1;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS get_peers_who_did_not_leave_campus(checking_date DATE);
CREATE OR REPLACE FUNCTION get_peers_who_did_not_leave_campus(checking_date DATE)
RETURNS SETOF VARCHAR AS $$
SELECT DISTINCT Peer
FROM TimeTracking 
GROUP BY Peer, Date
HAVING COUNT(state) = 1 OR COUNT(state) = 2 AND Date = $1;
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION trigger_xp()
RETURNS TRIGGER AS
$$
BEGIN
    if new.xpamount > (SELECT maxxp FROM tasks
					   JOIN checks ON tasks.title = checks.task
					   WHERE checks.id = new."check") 
					   OR
					  (SELECT state FROM p2p
					   JOIN checks ON checks.id = p2p."check"
					   WHERE checks.id = new."check" AND state IN ('Success', 'Failure')) <> 'Success' 
					   OR
					  (EXISTS(SELECT state FROM verter 
							  JOIN checks ON checks.id = verter."check"
							  WHERE checks.id = new."check") AND
					  (SELECT state FROM verter
					   JOIN checks ON checks.id = verter."check"
					   WHERE checks.id = new."check" AND (state IN('Success', 'Failure'))) <> 'Success')
THEN
		RAISE NOTICE 'xp_amount: %', new.xpamount;
		RAISE NOTICE 'max_xp: %', (SELECT maxxp FROM tasks 
								   JOIN checks ON tasks.title = checks.task 
								   WHERE checks.id = new."check");
        RAISE EXCEPTION 'Некорректный ввод хр';
	ELSE
		RAISE NOTICE 'xp_amount: %', new.xpamount;
		RAISE NOTICE 'max_xp: %', (SELECT maxxp FROM tasks 
								   JOIN checks ON tasks.title = checks.task 
								   WHERE checks.id = new."check");
		RETURN new;	
    END IF;
END;
$$ language plpgsql;


CREATE OR REPLACE TRIGGER check_xp_add
    BEFORE INSERT ON xp
    FOR EACH ROW 
	EXECUTE procedure trigger_xp();

CREATE OR REPLACE TRIGGER check_xp_add_2
    BEFORE INSERT ON xp
    FOR EACH ROW 
	EXECUTE procedure trigger_xp();

-- 4.1 Создать хранимую процедуру, которая, не уничтожая базу данных,
-- уничтожает все те таблицы текущей базы данных, имена которых начинаются с фразы 'TableName'.
CREATE TABLE TableName1 (name VARCHAR);
CREATE TABLE TableName2 (name VARCHAR);

CREATE OR REPLACE PROCEDURE Drop_Tables_TableName() AS 
$$
DECLARE
    table_drop TEXT; 
BEGIN
    FOR table_drop IN (SELECT tablename FROM pg_tables 
					   WHERE tablename LIKE 'tablename%')
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || table_drop || ' CASCADE;';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

call Drop_Tables_TableName();

-- SELECT * FROM pg_tables;
-- SELECT * FROM information_schema.function

-- 4.2 Создать хранимую процедуру с выходным параметром, которая выводит список
-- имен и параметров всех скалярных SQL функций пользователя в текущей базе данных.
-- Имена функций без параметров не выводить.
-- Имена и список параметров должны выводиться в одну строку.
-- Выходной параметр возвращает количество найденных функций.

CREATE OR REPLACE PROCEDURE scalar_functions(OUT f_count int) AS 
$$
DECLARE
    f_name TEXT;
    parameters TEXT;
BEGIN
	SELECT COUNT(*)
    INTO f_count
    FROM pg_proc
    WHERE proname NOT LIKE 'pg_%' AND pronargs > 0;
	
	FOR f_name, parameters IN 
		(SELECT pg_proc.proname AS function_name,
                pg_get_function_arguments(pg_proc.oid) as parameters
		 FROM pg_proc
		 WHERE  proname LIKE 'pg_%' AND pronargs > 0
		 ORDER BY 1)
    LOOP
		RAISE NOTICE 'Function: % (parameter %);', f_name, parameters;
	END LOOP;
END;
$$ LANGUAGE plpgsql;

call scalar_functions(null);
--4.3
-- Создать хранимую процедуру с выходным параметром, которая уничтожает 
-- все SQL DML триггеры в текущей базе данных. 
-- Выходной параметр возвращает количество уничтоженных триггеров.

CREATE OR REPLACE PROCEDURE del_triggers(OUT del_count integer) AS $$
DECLARE
    trig_name text;
BEGIN
    del_count := 0;
    FOR trig_name IN
        (SELECT trigger_name 
		 FROM information_schema.triggers
		 WHERE trigger_schema = 'public')
    LOOP
        EXECUTE 'DROP TRIGGER IF EXISTS ' || trig_name || ' ON ' || quote_ident(trig_name) || ' CASCADE';
        del_count = del_count + 1;
    END LOOP;
END;
$$ language plpgsql;

call del_triggers(null);

--4.4

CREATE OR REPLACE PROCEDURE search_obj(str TEXT) AS
$$
DECLARE
    obj_name TEXT;
    obj_description TEXT;
BEGIN
    FOR obj_name, obj_description IN (
        SELECT proname, obj_description(oid, 'pg_proc')
        FROM pg_proc
        WHERE proname ILIKE '%' || str || '%') --OR obj_description(oid, 'pg_proc') ILIKE '%' || str || '%')
    LOOP
        RAISE NOTICE 'NAME: %, 	||DESCRIPTION: %', obj_name, obj_description;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


call search_obj('zone');
