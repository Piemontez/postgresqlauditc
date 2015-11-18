-----------------------------
-- The trigger
-----------------------------

CREATE SCHEMA log;
 
CREATE TABLE log.audits
(
  id bigserial NOT NULL,
  owner_role CHARACTER VARYING(64) NOT NULL,
  owner_id CHARACTER VARYING(64) NOT NULL,
  operation CHARACTER(1) NOT NULL,
  FIELDS CHARACTER VARYING(64)[],
  DATA text[],
  TRANSACTION BIGINT,
  CONSTRAINT audits_pkey PRIMARY KEY (id)
);
 
CREATE OR REPLACE FUNCTION auditc() RETURNS TRIGGER AS
  'audit.so', 'auditc'
LANGUAGE c VOLATILE COST 1;

-----------------------------
-- Demo / example code
-----------------------------

CREATE TABLE teste_table
(
  id serial NOT NULL,
  teste_char VARCHAR(16) NOT NULL,
  teste_int INTEGER NOT NULL,
  CONSTRAINT teste_table_pkey PRIMARY KEY (id)
);
 
 
CREATE TRIGGER audit_teste_table
          BEFORE INSERT OR DELETE OR UPDATE ON teste_table
          FOR EACH ROW  EXECUTE PROCEDURE auditc();
 
 
INSERT INTO teste_table (id, teste_char, teste_int) VALUES(10, 'Hello', 123);
INSERT INTO teste_table (id, teste_char, teste_int) VALUES(20, 'Good', 456);
UPDATE teste_table SET teste_char = 'World' WHERE id = 10;
UPDATE teste_table SET teste_char = 'Bye' WHERE id = 20;
 
SELECT id, owner_role, owner_id, operation, DATA, FIELDS FROM log.audits WHERE owner_role = 'public.teste_table' ORDER BY owner_id;
 
--RESULT:
-- id |     owner_role     | owner_id | operation |     DATA      |          FIELDS           
---------+--------------------+----------+-----------+---------------+---------------------------
-- 1  | public.teste_table | 10       | I         | {123,Hello,10} | {teste_int,teste_char,id}
-- 3  | public.teste_table | 10       | U         | {World}        | {teste_char}
-- 2  | public.teste_table | 20       | I         | {456,Good,20}  | {teste_int,teste_char,id}
-- 4  | public.teste_table | 20       | U         | {Bye}          | {teste_char}
