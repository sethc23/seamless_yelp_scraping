
CREATE EXTENSION IF NOT EXISTS plpythonu;

-- Function: z_next_free(text, text, text)
DROP FUNCTION IF EXISTS z_next_free(text, text, text) cascade;
CREATE OR REPLACE FUNCTION z_next_free(
    table_name text,
    uid_col text,
    _seq text)
  RETURNS integer AS
$BODY$
                stop=False
                T = {'tbl':table_name,'uid_col':uid_col,'_seq':_seq}
                p = """

                            select count(column_name) c
                            from INFORMATION_SCHEMA.COLUMNS
                            where table_name = '%(tbl)s'
                            and column_name = '%(uid_col)s';

                    """ % T
                cnt = plpy.execute(p)[0]['c']

                if cnt==0:
                    p = "create sequence %(tbl)s_%(uid_col)s_seq start with 1;"%T
                    t = plpy.execute(p)
                    p = "alter table %(tbl)s alter column %(uid_col)s set DEFAULT z_next_free('%(tbl)s'::text, 'uid'::text, '%(tbl)s_uid_seq'::text);"%T
                    t = plpy.execute(p)
                stop=False
                while stop==False:
                    p = "SELECT nextval('%(tbl)s_%(uid_col)s_seq') next_val"%T
                    try:
                        t = plpy.execute(p)[0]['next_val']
                    except plpy.spiexceptions.UndefinedTable:
                        p = "select max(%(uid_col)s) from %(tbl)s;" % T
                        max_num = plpy.execute(p)[0]['max']
                        if max_num:
                            T.update({'max_num':str(max_num)})
                        else:
                            T.update({'max_num':str(1)})
                        p = "create sequence %(tbl)s_%(uid_col)s_seq start with %(max_num)s;" % T
                        t = plpy.execute(p)
                        p = "SELECT nextval('%(tbl)s_%(uid_col)s_seq') next_val"%T
                        t = plpy.execute(p)[0]['next_val']
                    T.update({'next_val':t})
                    p = "SELECT count(%(uid_col)s) cnt from %(tbl)s where %(uid_col)s=%(next_val)s"%T
                    chk = plpy.execute(p)[0]['cnt']
                    if chk==0:
                        stop=True
                        break
                return T['next_val']

                $BODY$
LANGUAGE plpythonu;

-- Function: z_get_seq_value()
drop function if exists z_get_seq_value(text) cascade;
create or replace function z_get_seq_value(seq_name text) returns integer as $$
declare x int;
begin
x = currval(seq_name::regclass)+1;
return x;
exception
    when sqlstate '42P01' then return 1;
    when sqlstate '55000' then return next_val(seq_name::regclass);
end;
$$ language plpgsql;

-- Trigger Function: z_auto_add_primary_key()

DROP FUNCTION IF EXISTS z_auto_add_primary_key() cascade;
CREATE OR REPLACE FUNCTION z_auto_add_primary_key()
  RETURNS event_trigger AS
$BODY$
DECLARE
    has_index boolean;
    tbl_name text;
    primary_key_col text;
    missing_primary_key boolean;
    has_uid_col boolean;
    _seq text;
BEGIN
    select relhasindex,relname into has_index,tbl_name
        from pg_class
        where relnamespace=2200
        and relkind='r'
        order by oid desc limit 1;
    IF (
        pg_trigger_depth()=0
        and has_index=False )
    THEN
        --RAISE NOTICE 'NOT HAVE INDEX';
        EXECUTE format('SELECT a.attname
                        FROM   pg_index i
                        JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                             AND a.attnum = ANY(i.indkey)
                        WHERE  i.indrelid = ''%s''::regclass
                        AND    i.indisprimary',tbl_name)
        INTO primary_key_col;
        
        missing_primary_key = (select primary_key_col is null);

        IF missing_primary_key=True
        THEN
            --RAISE NOTICE 'IS MISSING PRIMARY KEY';
            _seq = format('%I_uid_seq',tbl_name);
            EXECUTE format('select count(*)!=0 
                        from INFORMATION_SCHEMA.COLUMNS 
                        where table_name = ''%s''
                        and column_name = ''uid''',tbl_name)
            INTO has_uid_col;            
            IF (has_uid_col=True)
            THEN
                --RAISE NOTICE 'HAS UID COL';
                execute format('alter table %I 
                                    alter column uid type integer,
                                    alter column uid set not null,
                                    alter column uid set default z_next_free(
                                        ''%I'',
                                        ''uid'',
                                        ''%I''), 
                                    ADD PRIMARY KEY (uid);',tbl_name,tbl_name,_seq);
            ELSE
                --RAISE NOTICE 'NOT HAVE UID COL';
                _seq = format('%I_uid_seq',tbl_name);
                execute format('alter table %I add column uid integer primary key
                                default z_next_free(''%I'',''uid'',''%I'')',
                                tbl_name,tbl_name,_seq);
            END IF;
            
        END IF;

    END IF;
    
END;
$BODY$
  LANGUAGE plpgsql;

DROP EVENT TRIGGER if exists missing_primary_key_trigger;
CREATE EVENT TRIGGER missing_primary_key_trigger
ON ddl_command_end
WHEN TAG IN ('CREATE TABLE','CREATE TABLE AS')
EXECUTE PROCEDURE z_auto_add_primary_key();



-- Trigger Function: z_auto_add_last_updated_field()

DROP FUNCTION if exists z_auto_add_last_updated_field() cascade;
CREATE OR REPLACE FUNCTION z_auto_add_last_updated_field()
  RETURNS event_trigger AS
$BODY$
DECLARE
    last_table text;
    has_last_updated boolean;
BEGIN
    last_table := ( select relname from pg_class
                    where relnamespace=2200
                    and relkind='r'
                    order by oid desc limit 1);

    SELECT count(*)>0 INTO has_last_updated FROM information_schema.columns
        where table_name='||quote_ident(last_table)||'
        and column_name='last_updated';

    IF (
        pg_trigger_depth()=0
        and has_last_updated=False
        and position('tmp_' in last_table)=0 )
    THEN
        execute format('alter table %I drop column if exists last_updated',last_table);
        execute format('alter table %I add column last_updated timestamp with time zone',last_table);
        execute format('DROP FUNCTION if exists z_auto_update_timestamp_on_%s_in_last_updated() cascade',last_table);
        execute format('DROP TRIGGER if exists update_timestamp_on_%s_in_last_updated ON %s',last_table,last_table);

        execute format('CREATE OR REPLACE FUNCTION z_auto_update_timestamp_on_%s_in_last_updated()'
                        || ' RETURNS TRIGGER AS $$'
                        || ' BEGIN'
                        || '     NEW.last_updated := now();'
                        || '     RETURN NEW;'
                        || ' END;'
                        || ' $$ language ''plpgsql'';'
                        || '',last_table);

        execute format('CREATE TRIGGER update_timestamp_on_%s_in_last_updated'
                        || ' BEFORE UPDATE OR INSERT ON %I'
                        || ' FOR EACH ROW'
                        || ' EXECUTE PROCEDURE z_auto_update_timestamp_on_%s_in_last_updated();'
                        || '',last_table,last_table,last_table);

    END IF;

END;
$BODY$
  LANGUAGE plpgsql;

DROP EVENT TRIGGER if exists missing_last_updated_field;
CREATE EVENT TRIGGER missing_last_updated_field
ON ddl_command_end
WHEN TAG IN ('CREATE TABLE','CREATE TABLE AS')
EXECUTE PROCEDURE z_auto_add_last_updated_field();

DROP FUNCTION IF EXISTS json_object_set_keys(json,text[],anyarray);
CREATE OR REPLACE FUNCTION "json_object_set_keys"(
  "json"          json,
  "keys_to_set"   TEXT[],
  "values_to_set" anyarray
)
  RETURNS json
  LANGUAGE sql
  IMMUTABLE
  STRICT
AS $function$
SELECT concat('{', string_agg(to_json("key") || ':' || "value", ','), '}')::json
FROM (SELECT *
      FROM json_each("json")
     WHERE "key" <> ALL ("keys_to_set")
     UNION ALL
    SELECT DISTINCT ON ("keys_to_set"["index"])
           "keys_to_set"["index"],
           CASE
             WHEN "values_to_set"["index"] IS NULL THEN 'null'::json
             ELSE to_json("values_to_set"["index"])
           END
      FROM generate_subscripts("keys_to_set", 1) AS "keys"("index")
      JOIN generate_subscripts("values_to_set", 1) AS "values"("index")
     USING ("index")) AS "fields"
$function$;

CREATE OR REPLACE FUNCTION public.json_append(data json, insert_data json)
RETURNS json
IMMUTABLE
LANGUAGE sql
AS $$
    SELECT ('{'||string_agg(to_json(key)||':'||value, ',')||'}')::json
    FROM (
        SELECT * FROM json_each(data)
        UNION ALL
        SELECT * FROM json_each(insert_data)
    ) t;
$$;
 
CREATE OR REPLACE FUNCTION public.json_delete(data json, keys text[])
RETURNS json
IMMUTABLE
LANGUAGE sql
AS $$
    SELECT ('{'||string_agg(to_json(key)||':'||value, ',')||'}')::json
    FROM (
        SELECT * FROM json_each(data)
        WHERE key <>ALL(keys)
    ) t;
$$;
 
CREATE OR REPLACE FUNCTION public.json_merge(data json, merge_data json)
RETURNS json
IMMUTABLE
LANGUAGE sql
AS $$
    SELECT ('{'||string_agg(to_json(key)||':'||value, ',')||'}')::json
    FROM (
        WITH to_merge AS (
            SELECT * FROM json_each(merge_data)
        )
        SELECT *
        FROM json_each(data)
        WHERE key NOT IN (SELECT key FROM to_merge)
        UNION ALL
        SELECT * FROM to_merge
    ) t;
$$;
 
CREATE OR REPLACE FUNCTION public.json_update(data json, update_data json)
RETURNS json
IMMUTABLE
LANGUAGE sql
AS $$
    SELECT ('{'||string_agg(to_json(key)||':'||value, ',')||'}')::json
    FROM (
        WITH old_data AS (
            SELECT * FROM json_each(data)
        ), to_update AS (
            SELECT * FROM json_each(update_data)
            WHERE key IN (SELECT key FROM old_data)
        )
    SELECT * FROM old_data
    WHERE key NOT IN (SELECT key FROM to_update)
    UNION ALL
    SELECT * FROM to_update
) t;
$$;

CREATE OR REPLACE FUNCTION public.json_lint(from_json json, ntab integer DEFAULT 0)
RETURNS json
LANGUAGE sql
IMMUTABLE STRICT
AS $$
SELECT (CASE substring(from_json::text FROM '(?m)^[\s]*(.)') /* Get first non-whitespace */
        WHEN '[' THEN
                (E'[\n'
                        || (SELECT string_agg(repeat(E'\t', ntab + 1) || json_lint(value, ntab + 1)::text, E',\n') FROM json_array_elements(from_json)) ||
                E'\n' || repeat(E'\t', ntab) || ']')
        WHEN '{' THEN
                (E'{\n'
                        || (SELECT string_agg(repeat(E'\t', ntab + 1) || to_json(key)::text || ': ' || json_lint(value, ntab + 1)::text, E',\n') FROM json_each(from_json)) ||
                E'\n' || repeat(E'\t', ntab) || '}')
        ELSE
                from_json::text
END)::json
$$;

CREATE OR REPLACE FUNCTION public.json_unlint(from_json json)
RETURNS json
LANGUAGE sql
IMMUTABLE STRICT
AS $$
SELECT (CASE substring(from_json::text FROM '(?m)^[\s]*(.)') /* Get first non-whitespace */
    WHEN '[' THEN
        ('['
            || (SELECT string_agg(json_unlint(value)::text, ',') FROM json_array_elements(from_json)) ||
        ']')
    WHEN '{' THEN
        ('{'
            || (SELECT string_agg(to_json(key)::text || ':' || json_unlint(value)::text, ',') FROM json_each(from_json)) ||
        '}')
    ELSE
        from_json::text
END)::json
$$;