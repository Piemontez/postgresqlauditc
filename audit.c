/**
* Build commands
**/
//For compile: cc -fpic -I/usr/include/pgsql/server/ -c audit.c
//For create shared Lib: cc -shared -o audit.so audit.o
//Move "audit.so" to postgres lib folder

/**
* The Lib audit.c
**/

#include "postgres.h"
#include "executor/spi.h"
#include "commands/trigger.h"
#include "utils/array.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"
#include "access/htup_details.h"
 
#ifdef PG_MODULE_MAGIC
    PG_MODULE_MAGIC;
#endif
 
//#define DEBUGAUDIT
 
#define OID_VARCHAR_ARRAY 1015
 
extern Datum auditc(PG_FUNCTION_ARGS);
 
PG_FUNCTION_INFO_V1(auditc);
 
/**
 * Structs
 **/
 
typedef char* string;
 
typedef struct {
    string* fields;
    string* data;
    string* pks;
    uint32 pk_length;
    int size;
} diffret;
 
typedef struct {
    string key;
    uint32 pk_length;
    string *values;
} entryset;
 
static entryset* primaryKeys = 0;
static int tableUsed = 0;
static int tableAmount = 0;
 
/**
 * Audit function
 **/
 
Datum
auditc(PG_FUNCTION_ARGS)
{
    if (SPI_connect() == SPI_OK_CONNECT)
    {
/** **************************** BEGIN singleton *****************************************/
        if (!tableAmount)
        {
            SPIPlanPtr prep = SPI_prepare("select count(tablename) from pg_tables where schemaname = 'public'", 0, NULL);
            if (prep)
            {
                if (SPI_execute_plan(prep, NULL, NULL, true, 0))
                {
                    uint32 pk_length = SPI_processed;
                    if (pk_length > 0)
                    {
                        tableAmount = atoi(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
                    }
                }
            }
            primaryKeys = malloc(tableAmount * sizeof(entryset));
        }
/** ----------------------------  END singleton ---------------------------- **/
 
        TriggerData *trigdata = (TriggerData *) fcinfo->context;
        HeapTuple oldtuple;
        HeapTuple newtuple;
 
#ifdef DEBUGAUDIT
        display(trigdata);
#endif
        char operation;
        if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
        {
            operation = 'I';
            oldtuple = 0;
            newtuple = trigdata->tg_trigtuple;
        } else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
        {
            operation = 'U';
            oldtuple = trigdata->tg_trigtuple;
            newtuple = trigdata->tg_newtuple;
        } else {
            operation = 'D';
            oldtuple = trigdata->tg_trigtuple;
            newtuple = trigdata->tg_trigtuple;
        }
 
        diffret ret;
        TupleDesc description = trigdata->tg_relation->rd_att;
        ret.size = 0;
        ret.pks = 0;
        ret.pk_length = 0;
 
        uint32 colunsSize = HeapTupleHeaderGetNatts(newtuple->t_data);
        if (colunsSize > 0)
        {
            int pos;
            ret.fields = SPI_palloc(colunsSize * sizeof(string));
            ret.data = SPI_palloc(colunsSize * sizeof(string));
 
            string *pks = 0;
/** **************************** GET PKS *****************************************/
#ifdef DEBUGAUDIT
            displayconstraints(trigdata->tg_relation);
            elog(INFO, "_____CHECK_PRIMARY_KEYS________");
#endif
 
/** **************************** CHECK PKS IN singleton *****************************************/
            {
                int row;
                for (row = 0; row < tableUsed; row++)
                {
#ifdef DEBUGAUDIT
                    elog(INFO, "pk: %s", primaryKeys[row].key);
#endif
 
                    if (strcmp(primaryKeys[row].key, SPI_getrelname(trigdata->tg_relation)) == 0)
                    {
                        ret.pk_length = primaryKeys[row].pk_length;
                        pks = primaryKeys[row].values;
                    }
                }
            }
/** ---------------------------- END CHECK PKS IN singleton ---------------------------- **/
 
/** **************************** CHECK PKS IN Data BASE *****************************************/
            if (!pks)
            {
#ifdef DEBUGAUDIT
            elog(INFO, "_____LOADING_PRIMARY_KEYS______");
#endif
 
                Oid oids[1] = {VARCHAROID};
                SPIPlanPtr prep = SPI_prepare("SELECT a.attname AS keys FROM pg_class c, pg_attribute a, pg_index i"
                  " WHERE c.relname = $1 AND c.oid=i.indrelid AND a.attnum > 0"
                  " AND a.attrelid = i.indexrelid AND i.indisprimary='t' ORDER BY a.attname ASC", 1, oids);
                if (prep)
                {
                    Datum * parameters = (Datum *) SPI_palloc(sizeof(Datum));
                    parameters[0] = CStringGetDatum(cstring_to_text(SPI_getrelname(trigdata->tg_relation)));
 
                    if (SPI_execute_plan(prep, parameters, NULL, true, 0))
                    {
                        ret.pk_length = SPI_processed;
                        if (ret.pk_length > 0)
                        {
                            pks = malloc(ret.pk_length * sizeof(string));
                            int row;
                            for (row = 0; row < ret.pk_length; row++)
                            {
                                string value = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, 1);
 
                                int size = strlen(value) + 1;
                                pks[row] = malloc(size);
                                memcpy( pks[row], value, size);
 
                                SPI_pfree(value);
#ifdef DEBUGAUDIT
                            elog(INFO, "pk: %s", pks[row]);
#endif
                            }
                        }
                        if (tableUsed < tableAmount)
                        {
                            char* tableName = SPI_getrelname(trigdata->tg_relation);
                            int size = strlen(tableName) + 1;
                            primaryKeys[tableUsed].key = malloc(size);
                            memcpy( primaryKeys[tableUsed].key, tableName, size);
 
                            primaryKeys[tableUsed].pk_length = ret.pk_length;
                            primaryKeys[tableUsed].values = pks;
                            tableUsed++;
                        }
                        SPI_freetuptable(SPI_tuptable);
                    }
                }
            }
/** ---------------------------- END CHECK PKS IN Data BASE ---------------------------- **/
 
            if (ret.pk_length == 0)
            {
                elog(ERROR,"auditc.so: Erro find primary key");
            }
/** ---------------------------- GET PKS ---------------------------- **/
            int pk_pos;
 
            ret.pks = SPI_palloc(ret.pk_length * sizeof(string));
 
/** **************************** DIFF *****************************************/
            string column;
            string newValue;
            for(pos = colunsSize; pos > 0; pos--)
            {
                column = SPI_fname(description, pos);
                newValue = SPI_getvalue(newtuple, description, pos);
                for(pk_pos = 0; pk_pos < ret.pk_length; pk_pos++)
                {
                    if (strcmp(column, pks[pk_pos]) == 0)
                    {
                        ret.pks[pk_pos] = newValue;
                    }
                }
                if (oldtuple != 0)
                {
                    string oldValue = SPI_getvalue(oldtuple, description, pos);
                    if ((oldValue == 0 && newValue == 0)
                        || (
                        !(oldValue == 0 && newValue != 0)
                        && !(oldValue != 0 && newValue == 0)
                        && !strcmp(oldValue, newValue)))
                    {
                        continue;
                    }
                }
 
                ret.fields[ret.size] = column;
                ret.data[ret.size] = newValue;
                ret.size++;
            }
 
/** ---------------------------- END DIFF ---------------------------- **/
        } else {
            ret.size = 0;
        }
 
#ifdef DEBUGAUDIT
    displaydiff(&ret);
#endif
 
/** **************************** SAVE IN LOG *****************************************/
        if (ret.size > 0 || operation == 'D')
        {
            Oid oids[6] = {VARCHAROID, VARCHAROID, CHAROID, OID_VARCHAR_ARRAY, TEXTARRAYOID, INT4OID};
            SPIPlanPtr prep = SPI_prepare("INSERT INTO log.audits"
                " (transaction, owner_role, owner_id, operation, fields, data)"
                " VALUES (txid_current(), $1, $2, $3, $4, $5)", 5, oids);
            if (prep)
            {
		//OWNER_ROLE COLUMN
                char *schema = SPI_getnspname(trigdata->tg_relation);
                char *table = SPI_getrelname(trigdata->tg_relation);
                char *st = (char *) SPI_palloc(strlen(schema) + strlen(table) + 2); /* +1 for null character */
                strcpy(st, schema);
                strcat(st, ".");
                strcat(st, table);
 
		//OWNER_ID COLUMN
                int size = strlen(ret.pks[0]);
                int pk_pos;
                for(pk_pos = 1; pk_pos < ret.pk_length; pk_pos++)
                {
                    size += strlen(ret.pks[pk_pos]) + 1;
                }
 
                char *pkIds = (char *) SPI_palloc(size * sizeof(char) + 1); /* +1 for null character */
                strcpy(pkIds, ret.pks[0]);
                for(pk_pos = 1; pk_pos < ret.pk_length; pk_pos++)
                {
                    char *val= ret.pks[pk_pos];
                    strcat(pkIds, ",");
                    strcat(pkIds, val);
                }
 
		//DATA COLUMN and FIELDS COLUMN
                Datum *fields = SPI_palloc(sizeof(Datum) * ret.size);
                Datum *values = SPI_palloc(sizeof(Datum) * ret.size);
                bool *valuesNulls = SPI_palloc(sizeof(bool) * ret.size);
                {
                    int pk_pos;
                    for(pk_pos = 0; pk_pos < ret.size; pk_pos++)
                    {
                        fields[pk_pos] = CStringGetDatum(cstring_to_text(ret.fields[pk_pos]));
                        if (ret.data[pk_pos]) {
                            values[pk_pos] = CStringGetDatum(cstring_to_text(ret.data[pk_pos]));
                            valuesNulls[pk_pos] = false;
                        } else {
                            values[pk_pos] = 0;
                            valuesNulls[pk_pos] = true;
                        }
                    }
                }
 
		//PARAMETERS For statement
                Datum *parameters = SPI_palloc(sizeof(Datum) * 6);
                parameters[0] = CStringGetDatum(cstring_to_text(st));
                parameters[1] = CStringGetDatum(cstring_to_text(pkIds));
                parameters[2] = CharGetDatum(operation);
                parameters[3] = (Datum) construct_array(fields, ret.size, VARCHAROID, -1, false, 'i');
                {
                    int	* dims = SPI_palloc(sizeof(int));
                    dims[0] = ret.size;
 
                    int	* lbs = SPI_palloc(sizeof(int));
                    lbs[0] = 1;
 
                    parameters[4] = (Datum) construct_md_array(values, valuesNulls, 1, dims, lbs, VARCHAROID, -1, false, 'i');
                }
 
		//NULL VALUES
                char *nulls = SPI_palloc(sizeof(char) * 6);
                nulls[0] = ' ';
                nulls[1] = ' ';
                nulls[2] = ' ';
                nulls[3] = ret.size ? ' ' : 'n';
                nulls[4] = ret.size ? ' ' : 'n';
 
		//EXECUTE QUERY
                SPI_execute_plan(prep, parameters, nulls, false, 0);
 
		//CLEAN VARIABLES
                SPI_pfree(st);
                SPI_pfree(pkIds);
                SPI_pfree(parameters);
                SPI_pfree(nulls);
            } else {
                elog(ERROR,"auditc.so: Erro to create preparar: %d", SPI_result);
            }
        }
/** ---------------------------- END IN LOG  ---------------------------- **/
	//CLEAN VARIABLES
        SPI_pfree(ret.pks);
        SPI_pfree(ret.data);
        SPI_pfree(ret.fields);
 
        if (SPI_finish() != SPI_OK_FINISH)
        {
            elog(ERROR,"auditc.so: Erro to close connection");
        }
        return PointerGetDatum(newtuple);
    } else {
        elog(ERROR,"auditc.so: Erro not get Connection");
    }
}
 
/**
 * Debug functions
 **/
#ifdef DEBUGAUDIT
display(TriggerData *trigdata)
{
    TupleDesc tupdesc = trigdata->tg_relation->rd_att;
 
    elog(INFO,"_________TRIGGER_INFO___________");
    elog(INFO,"nspname(schema) = %s", SPI_getnspname(trigdata->tg_relation));
    elog(INFO,"relname(table)  = %s", SPI_getrelname(trigdata->tg_relation));
    elog(INFO,"_________TRIG_TUPLE_____________");
    displayvars(trigdata->tg_trigtuple, tupdesc);
    if (!TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
    {
        elog(INFO,"_________TRIG_NEW_______________");
        displayvars(trigdata->tg_newtuple, tupdesc);
    }
}
 
displayvars(HeapTuple tupe, HeapTuple tupdesc)
{
    if (!tupe) return;
    uint32 coluns = HeapTupleHeaderGetNatts(tupe->t_data);
    elog(INFO,"colums size: %d",coluns);
    if (coluns > 0)
    {
        bool isnull = false;
        int pos;
        for(pos = coluns; pos > 0; pos--)
        {
            char* value = SPI_getvalue(tupe, tupdesc, pos);
            elog(INFO,"column %s value: %s",SPI_fname(tupdesc, pos), (value ? value : "(null)"));
        }
    }
}
displaydiff(diffret *ret)
{
    elog(INFO,"_________DIFF_VALUES____________");
    elog(INFO,"colums diff: %d",ret->size);
    if (ret->size < 1) return;
    int pos;
    for(pos = 0; pos < ret->size; pos++)
    {
        elog(INFO,"column %s diff: %s", ret->fields[pos], ret->data[pos]);
    }
}
displayconstraints(Relation relation)
{
    elog(INFO, "______CONSTRAINTS_OIDs_________");
    ListCell    *i;
    foreach(i, relation->rd_indexlist)
    {
        Oid oid = lfirst(i);
        elog(INFO, "Oid: %d", oid);
    }
    elog(INFO, "______CONSTRAINTS_INDEX________");
    elog(INFO, "rd_index: %d", relation->rd_index);
    if (relation->rd_index)
    {
        elog(INFO, "indkey.ndim: %d", relation->rd_index->indkey.ndim);
        elog(INFO, "indkey.vl_len_: %d", relation->rd_index->indkey.vl_len_);
    }
}
 
#endif
