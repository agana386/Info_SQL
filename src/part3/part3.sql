-- 1) Написать функцию, возвращающую таблицу TransferredPoints в более человекочитаемом виде

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

select * from get_transferred_points_hr();

-- 2) Написать функцию, которая возвращает таблицу вида: ник пользователя, название проверенного задания, кол-во полученного XP

DROP FUNCTION IF EXISTS get_checked_task();
CREATE OR REPLACE FUNCTION get_checked_task()
RETURNS TABLE (Peer VARCHAR, Task VARCHAR, XP INT) AS $$
BEGIN
	RETURN QUERY
	SELECT Checks.Peer,
		   Checks.Task,
		   XP.XPAmount
	FROM P2P 
	INNER JOIN Checks ON P2P."check" = Checks.ID
	INNER JOIN XP ON XP."check" = Checks.ID
	INNER JOIN Verter ON Verter."check" = Checks.ID
	WHERE P2P.State = 'Success' AND Verter.State = 'Success';
END;
$$ LANGUAGE plpgsql;

select * from get_checked_task();

-- 3) Написать функцию, определяющую пиров, которые не выходили из кампуса в течение всего дня

DROP FUNCTION IF EXISTS get_peers_who_did_not_leave_campus(checking_date DATE);
CREATE OR REPLACE FUNCTION get_peers_who_did_not_leave_campus(checking_date DATE)
RETURNS SETOF VARCHAR AS $$
SELECT DISTINCT Peer
FROM TimeTracking 
GROUP BY Peer, Date
HAVING COUNT(state) = 1 OR COUNT(state) = 2 AND Date = $1;
$$ LANGUAGE sql;

select * from get_peers_who_did_not_leave_campus('2023-08-11')

-- 4) Посчитать изменение в количестве пир поинтов каждого пира по таблице TransferredPoints

DROP TABLE IF EXISTS PointsChanges;
CREATE TABLE IF NOT EXISTS PointsChanges (
	Peer VARCHAR,
	PointsChange INT
);


DROP PROCEDURE IF EXISTS peer_points_changes();
CREATE OR REPLACE PROCEDURE peer_points_changes() AS $$
	WITH checking_peer AS (
    SELECT checkingpeer, SUM(pointsamount) AS got_points
    FROM transferredpoints
    GROUP BY checkingpeer),

    checked_peer AS (
        SELECT checkedpeer, SUM(pointsamount) AS given_points
        FROM transferredpoints
        GROUP BY checkedpeer)
		
	INSERT INTO PointsChanges
    SELECT checkedpeer AS Peer, ((COALESCE(got_points, 0)) - (COALESCE(given_points, 0))) AS pointschange
    FROM (SELECT *
          FROM checking_peer FULL JOIN checked_peer
                             ON checkingpeer = checkedpeer) AS res_table
    ORDER BY pointschange;
$$ LANGUAGE sql;

CALL peer_points_changes();
SELECT * FROM PointsChanges
ORDER BY PointsChange DESC;

-- 5) Посчитать изменение в количестве пир поинтов каждого пира по таблице, возвращаемой первой функцией из Part 3

DROP TABLE PointsChanges;
CREATE TABLE IF NOT EXISTS PointsChanges (
	Peer VARCHAR,
	PointsChange INT
);


DROP PROCEDURE IF EXISTS peer_points_changes_p1();
CREATE OR REPLACE PROCEDURE peer_points_changes_p1() AS $$
	WITH tmp1 AS (
			SELECT *, CASE
						WHEN pointsamount < 0
						THEN
							pointsamount * (-1)
						ELSE
							pointsamount
						END AS new_points
			FROM get_transferred_points_hr()
		),
		checking_peer AS (
		SELECT peer1, SUM(new_points) AS got_points
		FROM tmp1
		GROUP BY peer1
		),
		checked_peer AS (
			SELECT peer2, SUM(new_points) AS given_points
			FROM tmp1
			GROUP BY peer2)
			
		INSERT INTO PointsChanges
		SELECT COALESCE (peer1, peer2) AS Peer, ((COALESCE(got_points, 0)) - (COALESCE(given_points, 0))) AS pointschange
		FROM (SELECT *
			  FROM checking_peer FULL JOIN checked_peer
								 ON peer1 = peer2) AS res_table
$$ LANGUAGE sql;
CALL peer_points_changes_p1();
SELECT * FROM PointsChanges
ORDER BY PointsChange DESC;

-- 6) Определить самое часто проверяемое задание за каждый день

DROP TABLE IF EXISTS PopularTasks;
CREATE TABLE IF NOT EXISTS PopularTasks (
	"Day"  DATE,
	"Task" VARCHAR
);


DROP PROCEDURE IF EXISTS most_checked_task();
CREATE OR REPLACE PROCEDURE most_checked_task() AS $$
	WITH tmp1 AS   (SELECT Checks.Date AS "Day", Checks.Task AS "Task", COUNT(*) AS "counter"
					            FROM Checks
					            GROUP BY 1, 2),
			          tmp2 AS   (SELECT tmp1."Day", MAX("counter") AS "Days"
					            FROM tmp1
					            GROUP BY 1)
		INSERT INTO PopularTasks
		SELECT tmp1."Day", tmp1."Task"
		FROM tmp1
		JOIN tmp2 ON tmp1."Day" = tmp2."Day" AND tmp1."counter" = tmp2."Days"
		ORDER BY 1;
$$ LANGUAGE sql;
CALL most_checked_task();
SELECT * FROM PopularTasks;

-- 7) Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания

DROP TABLE IF EXISTS PeersMakesAllTasksInBlock;
CREATE TABLE IF NOT EXISTS PeersMakesAllTasksInBlock (
	Peer VARCHAR,
	day DATE
);


DROP PROCEDURE IF EXISTS peers_done_task_block(block VARCHAR);
CREATE OR REPLACE PROCEDURE peers_done_task_block(block VARCHAR) AS $$
	INSERT INTO PeersMakesAllTasksInBlock
	SELECT peer, "date" AS Day
    FROM checks JOIN p2p
    ON checks.id = p2p."check"
    LEFT JOIN verter
    ON checks.id = verter."check"
    WHERE p2p.state = 'Success' AND (verter.state = 'Success' OR verter.state IS NULL)
        AND task = (SELECT MAX(title) FROM tasks
                                        WHERE title ~ ('^' || $1 ||'[0-9]+_{1}'))
    ORDER BY Day DESC;
$$ LANGUAGE sql;

CALL peers_done_task_block('C');
SELECT * FROM PeersMakesAllTasksInBlock;

--3.8 пределить, к какому пиру стоит идти на проверку каждому обучающемуся
-- Определять нужно исходя из рекомендаций друзей пира, т.е. нужно найти пира, 
-- проверяться у которого рекомендует наибольшее число друзей. 
-- Формат вывода: ник пира, ник найденного проверяющего

DROP FUNCTION IF EXISTS recommend_peer();

CREATE OR REPLACE FUNCTION recommend_peer()
RETURNS TABLE(Peer VARCHAR, RecommendedPeer VARCHAR) AS $$
BEGIN
	RETURN QUERY
		WITH recom_friends AS (
			SELECT Recommendations.Peer, 
				Recommendations.RecommendedPeer, 
				COUNT(Recommendations.RecommendedPeer) AS recoms
			FROM Recommendations
			GROUP BY Recommendations.Peer, Recommendations.RecommendedPeer),
		counts_recom AS (
			SELECT recom_friends.RecommendedPeer, 
				COUNT(recom_friends.RecommendedPeer) AS all_recoms, 
				Friends.Peer1 AS Peer
			FROM recom_friends
			LEFT JOIN Friends ON recom_friends.Peer = Friends.Peer2
			WHERE Friends.Peer1 != recom_friends.RecommendedPeer
			GROUP BY recom_friends.RecommendedPeer, recom_friends.Peer, Friends.Peer1),
		result_out AS (
			SELECT counts_recom.Peer, 
				counts_recom.RecommendedPeer, 
				all_recoms, 
				ROW_NUMBER() OVER (PARTITION BY counts_recom.Peer ORDER BY COUNT(*) DESC) AS num
			FROM counts_recom
			WHERE all_recoms = (SELECT MAX(all_recoms) FROM counts_recom) 
				AND counts_recom.Peer != counts_recom.RecommendedPeer
			GROUP BY counts_recom.Peer, counts_recom.RecommendedPeer, all_recoms
			ORDER BY 1)
	SELECT result_out.Peer, result_out.RecommendedPeer
	FROM result_out
	WHERE num = 1;
END;
$$ LANGUAGE plpgsql;

select * from recommend_peer();


-- 9) Определить процент пиров, которые:
-- Приступили только к блоку 1
-- Приступили только к блоку 2
-- Приступили к обоим
-- Не приступили ни к одному

DROP TABLE IF EXISTS TempTableTask9;
CREATE TABLE IF NOT EXISTS TempTableTask9 (
	StartedBlock1       INT,
	StartedBlock2       INT,
	StartedBothBlocks   INT,
	DidntStartAnyBlock  INT
);

-- DROP PROCEDURE IF EXISTS block_start_percentage(block1 VARCHAR, block2 VARCHAR);
CREATE OR REPLACE PROCEDURE block_start_percentage("block1" VARCHAR, "block2" VARCHAR)
AS
$$
DECLARE
    "all_peers" BIGINT;
BEGIN
    all_peers := (SELECT count(Nickname) FROM Peers);
	WITH block1_users AS        (SELECT DISTINCT Peer
								FROM Checks
								WHERE Task ~ ('^' || $1 ||'[0-9]+_{1}')
								),

		 block2_users AS        (SELECT DISTINCT Peer
								FROM Checks
								WHERE Task ~ ('^' || $2 ||'[0-9]+_{1}')
								),

		 both_blocks_users AS   (SELECT Peer
								FROM block1_users
								INTERSECT
								SELECT Peer
								FROM block2_users
								),

		 neither_block_users AS (SELECT Nickname AS Peer
								FROM Peers
								EXCEPT
								(SELECT Peer
								FROM block1_users
								UNION DISTINCT
								SELECT Peer
								FROM block2_users)
								)
	INSERT INTO TempTableTask9
    SELECT ((SELECT count(Peer) FROM block1_users)::numeric/all_peers*100)::int,
           ((SELECT count(Peer) FROM block2_users)::numeric/all_peers*100)::int,
           ((SELECT count(Peer) FROM both_blocks_users)::numeric/all_peers*100)::int,
           ((SELECT count(Peer) FROM neither_block_users)::numeric/all_peers*100)::int;
END
$$ LANGUAGE plpgsql;

CALL block_start_percentage('C', 'DO');
SELECT * FROM TempTableTask9;

-- 10) Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения

DROP TABLE IF EXISTS TempTableTask10;
CREATE TABLE IF NOT EXISTS TempTableTask10 (
	SuccessfulChecks      INT,
	UnsuccessfulChecks    INT
);

DROP PROCEDURE IF EXISTS birthday_check();
CREATE OR REPLACE PROCEDURE birthday_check()
AS
$$
BEGIN
	WITH counts AS (SELECT COUNT(Checks.Peer)
					FILTER (WHERE P2P.State = 'Success') AS success,
					COUNT(Checks.Peer)
					FILTER (WHERE P2P.State= 'Failure') AS fail
					FROM Checks
					LEFT JOIN Peers ON Checks.Peer = Peers.Nickname
					JOIN P2P ON Checks.ID = P2P.Check
					WHERE (EXTRACT(MONTH FROM Checks.Date) = EXTRACT(MONTH FROM Peers.Birthday))
						AND (EXTRACT(DAY FROM Checks.Date) = EXTRACT(DAY FROM Peers.Birthday)))
	INSERT INTO TempTableTask10
	SELECT  (success::NUMERIC / NULLIF(success + fail, 0)::NUMERIC *100)::INT AS SuccessfulChecks,
			(fail::NUMERIC / NULLIF(success + fail, 0)::NUMERIC * 100)::INT AS UnsuccessfulChecks
	FROM counts;
END
$$ LANGUAGE plpgsql;

CALL birthday_check();
SELECT * FROM TempTableTask10;

-- 11) Определить всех пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3

DROP TABLE IF EXISTS TempTableTask11;
CREATE TABLE IF NOT EXISTS TempTableTask11 (
	Peer      VARCHAR
);

DROP PROCEDURE IF EXISTS peers_done_1_2_but_not_3();
CREATE OR REPLACE PROCEDURE peers_done_1_2_but_not_3(task1 VARCHAR, task2 VARCHAR, task3 VARCHAR) AS
$$
BEGIN
        WITH success AS (SELECT peer, count(peer)
                         FROM (SELECT peer, task
                               FROM ((SELECT *
                                      FROM checks
                                      JOIN XP ON checks.id = XP."check"
                                      WHERE task = task1)
                                     UNION
                                     (SELECT *
                                      FROM checks
                                      JOIN XP ON checks.id = XP."check"
                                      WHERE task = task2)) t1
                               GROUP BY peer, task) t2
                         GROUP BY peer
                         HAVING count(peer) = 2)
						 
		INSERT INTO TempTableTask11		 
        (SELECT peer
        FROM success)
        EXCEPT
        (SELECT success.peer
        FROM success
        JOIN checks ON checks.peer = success.peer
        JOIN XP ON checks.id = XP."check"
        WHERE task = task3);
END;
$$
LANGUAGE plpgsql;

CALL peers_done_1_2_but_not_3('C2_SimpleBashUtils', 'C3_StringPlus', 'CPP7_MLP');
SELECT * FROM TempTableTask11;

-- 12) Используя рекурсивное обобщенное табличное выражение, для каждой задачи вывести кол-во предшествующих ей задач

DROP TABLE IF EXISTS TempTableTask12;
CREATE TABLE IF NOT EXISTS TempTableTask12 (
	Task      VARCHAR,
	PrevCount INT
);

DROP PROCEDURE IF EXISTS count_parent_tasks();

CREATE OR REPLACE PROCEDURE count_parent_tasks()
AS
$$
BEGIN
	WITH RECURSIVE parent AS (SELECT (SELECT title
									  FROM tasks
									  WHERE parenttask IS NULL) AS Task,
									  0 AS PrevCount
							  UNION ALL
							  SELECT tasks.title,
									 PrevCount + 1
							  FROM parent
							  JOIN tasks ON tasks.ParentTask = parent.Task)
									   
	INSERT INTO TempTableTask12
	SELECT *
	FROM parent;
END;
$$
LANGUAGE plpgsql;
	
CALL count_parent_tasks();
SELECT * FROM TempTableTask12;

-- 13) Найти "удачные" для проверок дни. День считается "удачным", если
-- в нем есть хотя бы N идущих подряд успешных проверки
-- Параметры процедуры: количество идущих подряд успешных проверок N. 
-- Временем проверки считать время начала P2P этапа. 
-- Под идущими подряд успешными проверками подразумеваются успешные проверки, между которыми нет неуспешных. 
-- При этом кол-во опыта за каждую из этих проверок должно быть не меньше 80% от максимального. 
-- Формат вывода: список дней
DROP FUNCTION IF EXISTS best_checks_days();

CREATE OR REPLACE FUNCTION best_checks_days(n int) 
RETURNS SETOF DATE AS
$$
DECLARE 
	l RECORD;
	l2 RECORD;
    i INT := 0;
BEGIN
	FOR l IN SELECT "date" FROM checks GROUP BY "date" ORDER BY "date"
	LOOP 
		FOR l2 IN SELECT * 
				FROM (SELECT peer, p2p."state", xp.xpamount, tasks.maxxp, p2p."time", "date" 
					  FROM checks 
					  JOIN p2p ON checks.id = p2p."check"
					  LEFT JOIN verter ON checks.id = verter."check"
					  JOIN tasks ON checks.task = tasks.title 
					  JOIN xp ON checks.id = xp."check"
					  WHERE p2p."state" != 'Start'
						AND xp.xpamount >= tasks.maxxp * 0.8
						AND (verter."state" = 'Success' OR verter."state" IS NULL)
					  ORDER BY 6, 5) AS tmp
				WHERE l."date" = tmp."date"
		LOOP
			IF l2."state" = 'Success' 
				THEN i := i + 1;
                    IF i = n THEN RETURN NEXT l2."date";
                        EXIT;
                    END IF;
                ELSE i := 0;
                END IF;
		END LOOP;
        i := 0;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


select * from best_checks_days(2);

--14) Определить пира с наибольшим количеством XP

DROP FUNCTION IF EXISTS peer_max_xp();

CREATE OR replace FUNCTION peer_max_xp()
RETURNS TABLE (Peer VARCHAR, XP BIGINT) AS
$$
BEGIN
	RETURN QUERY
		SELECT get_checked_task.peer, sum(get_checked_task.xp) AS xp
		FROM get_checked_task()
		GROUP BY 1
		ORDER BY 2 DESC
		LIMIT 1;
END;
$$ LANGUAGE plpgsql;

select * from  peer_max_xp(); 

--15) Определить пиров, приходивших раньше заданного времени не менее N раз за всё время

CREATE OR REPLACE FUNCTION peers_comes_earlier(f_time TIME, n INT)
RETURNS TABLE (peers VARCHAR)
AS $$
BEGIN
	RETURN QUERY
		SELECT peer	FROM TimeTracking 
		WHERE "time" < f_time
		GROUP BY peer
		HAVING COUNT(*) >= n;
END;
$$ LANGUAGE PLPGSQL;

select * from peers_comes_earlier('16:00', 3);

-- 16) ch_time_tracking проверяет при вставке что первая за день запись state = 1
-- и state (со значениями 1 или 2) каждой последующей за день записи не равняется предыдущей
--16 Определить пиров, выходивших за последние N дней из кампуса больше M раз
--Параметры процедуры: количество дней N, количество раз M. 
--Формат вывода: список пиров

DROP FUNCTION IF EXISTS get_time_tracking_leaves;
CREATE OR REPLACE FUNCTION get_time_tracking_leaves (N int, M int) RETURNS TABLE (Peer varchar) AS $$ BEGIN RETURN QUERY WITH dates AS (
        SELECT Date
        FROM TimeTracking tt
        WHERE Date >= CURRENT_DATE - N
        GROUP BY Date
    ),
    peer_count AS (
        SELECT tt.Peer,
            COUNT(*)
        FROM dates
            JOIN TimeTracking tt ON dates.Date = tt.Date
        GROUP BY tt.Peer
    )
SELECT pc.Peer
FROM peer_count pc
WHERE count > M;
END;
$$ LANGUAGE plpgsql;
select *
from get_time_tracking_leaves (1000, 2);

-- 17) Определить для каждого месяца процент ранних входов

-- DROP TABLE IF EXISTS TempTableTask17;
CREATE TABLE IF NOT EXISTS TempTableTask17 (
	Month        VARCHAR,
	EarlyEntries INT
);

CREATE OR REPLACE PROCEDURE calculate_early_entry_percentage()
AS
$$
BEGIN
	WITH m_entry AS (SELECT date_trunc('month', TimeTracking.Date) AS month_e,
					 COUNT(*)  AS total_entries,
					 COUNT(*) FILTER (WHERE EXTRACT(hour FROM TimeTracking.Time) < 12) AS early_entries
					 FROM TimeTracking
					 JOIN Peers ON TimeTracking.Peer = Peers.Nickname AND EXTRACT("month" FROM Peers.Birthday) = EXTRACT("month" FROM TimeTracking.Date)
					 WHERE TimeTracking.State = 1
					 GROUP BY date_trunc('month', TimeTracking.Date)
					)
	INSERT INTO TempTableTask17
	SELECT to_char(m_entry.month_e, 'Month'), round(100.0 * m_entry.early_entries / m_entry.total_entries, 2)
	FROM m_entry;
END;
$$ LANGUAGE plpgsql;

CALL calculate_early_entry_percentage();
SELECT * FROM TempTableTask17;



