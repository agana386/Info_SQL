--2.1
drop procedure if exists add_p2p;

CREATE OR REPLACE PROCEDURE add_p2p(checked_peer VARCHAR,
									checking_peer VARCHAR,
									task_title VARCHAR,
									status_p2p CHECK_STATUS,
									"time" timestamp default current_timestamp(0)) AS
$$
BEGIN
	IF status_p2p = 'Start' THEN
        INSERT INTO checks VALUES ((SELECT MAX(id)+1 FROM checks), 
								   checked_peer, 
								   task_title,
								   now());
        INSERT INTO p2p VALUES ((SELECT MAX(id)+1 FROM p2p),
								(SELECT MAX(id) FROM checks),
								checking_peer,
								status_p2p,
								"time");
    ELSE
        INSERT INTO p2p VALUES ((SELECT MAX(id)+1 FROM p2p),
								(SELECT "check" FROM p2p 
									 WHERE p2p.checkingpeer = add_p2p.checking_peer AND state = 'Start'
									 ORDER BY p2p.time DESC
									 LIMIT 1), 
								checking_peer,
								status_p2p, 
								"time");
    END IF;
END;
$$ language plpgsql;


CALL add_p2p('Dynarini', 'Oryadela', 'C2_SimpleBashUtils', 'Success');
CALL add_p2p('Oryadela', 'Dynarini', 'C4_Math', 'Start');

--2.2
drop procedure if exists add_verter;

CREATE OR REPLACE PROCEDURE add_verter(checked_peer VARCHAR,
									   task_title VARCHAR,
									   status_verter CHECK_STATUS,
									   "time" timestamp default current_timestamp(0)) AS
$$
BEGIN
	INSERT INTO verter VALUES ((SELECT MAX(id)+1 FROM verter),
							   (SELECT "check" FROM p2p
									JOIN checks ON checks.id = p2p."check"
									WHERE state = 'Success' 
										AND checks.peer = add_verter.checked_peer 
										AND checks.task = task_title
									ORDER BY p2p."time" DESC
									LIMIT 1), 
							   status_verter, 
							   "time");
END;
$$ language plpgsql;

CALL add_verter('Dynarini', 'C2_SimpleBashUtils', 'Start');

--2.3
drop function if exists trigger_p2p cascade;
drop trigger if exists check_p2p_add;

CREATE OR REPLACE FUNCTION 
RETURNS TRIGGER AS
$$
BEGIN
    IF new.state = 'Start' THEN
        IF EXISTS (SELECT * FROM TransferredPoints
				   WHERE checkingpeer = new.checkingpeer
				   		AND checkedpeer = (SELECT peer FROM checks
										   WHERE new."check" = checks.id)) THEN
			UPDATE transferredPoints
			SET pointsamount = pointsamount+1
			WHERE checkingpeer = new.checkingpeer
			AND checkedpeer = (SELECT peer FROM checks WHERE new."check" = checks.id);
        ELSE
            INSERT INTO TransferredPoints VALUES ((SELECT MAX(id)+1 FROM TransferredPoints),
												  (SELECT peer FROM checks WHERE new."check" = checks.id),
												  new.checkingpeer,
												  1);
		END IF;
    END IF;
    RETURN NEW;
END;
$$ language plpgsql;

CREATE OR REPLACE TRIGGER check_p2p_add
    AFTER INSERT ON p2p
    FOR EACH ROW 
	EXECUTE procedure trigger_p2p();
	
--2.4
-- Написать триггер: перед добавлением записи в таблицу XP, проверить корректность добавляемой записи
-- Запись считается корректной, если:
-- Количество XP не превышает максимальное доступное для проверяемой задачи
-- Поле Check ссылается на успешную проверку
-- Если запись не прошла проверку, не добавлять её в таблицу.

drop function if exists trigger_xp() cascade;
drop trigger if exists check_xp_add();

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

insert into xp values ((select max(id) + 1 from xp), 18, '200');
