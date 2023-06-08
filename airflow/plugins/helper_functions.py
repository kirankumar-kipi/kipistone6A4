from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
import logging

logger = logging.getLogger("AIRFLOW_APP")


def preprocess_data(ti):
    data = ti.xcom_pull(task_ids="fetchdata_raw_data", key="return_value")
    ti.xcom_push(key="cleaned_data", value=data)


def ingestion_to_snowflake(ti):
    conn = None
    try:
        sf_hook = SnowflakeHook(snowflake_conn_id="app_snowflake")
        data = ti.xcom_pull(task_ids="preprocess", key="cleaned_data")
        conn = sf_hook.get_conn()
        cursor = conn.cursor()
        print("Row count", len(data), "-", len(data[0]))
        insert_query = f"INSERT INTO CIO_MASTER_DB.public.loaded_raw_data ( as_of_date, eom_date, created_at_entity_id, customer_id, record_number, invoice_number, reference_number, document_name, description, state, invoice_date, gl_posting_date, due_date, date_fully_paid, when_modified, base_currency, transaction_currency, total_transaction_amount, total_amount, total_paid, total_due, revised_due_date, total_amount_base, total_paid_base, total_due_base) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.executemany(insert_query, data)
        conn.commit()
        conn.close()
    except Exception as e:
        print(str(e))
        if conn:
            conn.close()
        raise Exception(str(e))


def ingest_metadata(ti, **kwargs):
    d_type = kwargs["TYPE"]
    table_map = {
        "AR_DATA": "ARDETAILS_AUDIT",
        "PAYMENT_DATA": "PAYMENTDETAILS_AUDIT",
        "CUSTOMER_DATA": "CUSTOMERDETAILS_AUDIT",
    }
    conn = None
    try:
        sf_hook = SnowflakeHook(
            snowflake_conn_id="app_snowflake",
            database="PROD_DWH",
            schema="STAGING_SCHEMA",
        )
        conn = sf_hook.get_conn()
        cursor = conn.cursor()
        # insert_query = f"INSERT INTO PROD_DWH.CURATED_SCHEMA.METADATA_TABLE (FILE_NAME, IS_FLATTENED, FILE_TYPE, LAST_UPDATED_DATE) select  METADATA$FILENAME as FILE_NAME, FALSE, UPPER(split_part(FILE_NAME, '.', -1)) , METADATA$FILE_LAST_MODIFIED as LAST_UPDATED_DATE from @S3_STAGE (file_format => JSON_FILE_FORMAT, pattern=>'.*AR_DATA_.*.json');"

        insert_query = f"INSERT INTO PROD_AUDIT_DB.AUDIT_SCHEMA.{table_map[d_type]} (FILENAME, UPLOADTIME, ISFLATTENED) select  METADATA$FILENAME as FILENAME,  METADATA$FILE_LAST_MODIFIED as UPLOADTIME, FALSE from @S3_STAGE (file_format => JSON_FILE_FORMAT, pattern=>'.*ar_details_.*.json');"
        cursor.execute(insert_query)
        affected_rows = cursor.fetchall()[0][0]
        print(f"Ingested data to metadata - {affected_rows}")
        conn.commit()
        conn.close()
        return affected_rows
    except Exception as e:
        print(f"error in ingesting ar metadata {str(e)}")
        if conn:
            conn.close()
        raise Exception(str(e))


# def ingest_ar_data_metadata_info(ti):
#     conn = None
#     try:
#         sf_hook = SnowflakeHook(
#             snowflake_conn_id="app_snowflake",
#             database="PROD_DWH",
#             schema="STAGING_SCHEMA",
#         )
#         conn = sf_hook.get_conn()
#         cursor = conn.cursor()
#         # insert_query = f"INSERT INTO PROD_DWH.CURATED_SCHEMA.METADATA_TABLE (FILE_NAME, IS_FLATTENED, FILE_TYPE, LAST_UPDATED_DATE) select  METADATA$FILENAME as FILE_NAME, FALSE, UPPER(split_part(FILE_NAME, '.', -1)) , METADATA$FILE_LAST_MODIFIED as LAST_UPDATED_DATE from @S3_STAGE (file_format => JSON_FILE_FORMAT, pattern=>'.*AR_DATA_.*.json');"

#         insert_query = f"INSERT INTO PROD_AUDIT_DB.AUDIT_SCHEMA.ARDETAILS_AUDIT (FILENAME, UPLOADTIME, ISFLATTENED) select  METADATA$FILENAME as FILENAME,  METADATA$FILE_LAST_MODIFIED as UPLOADTIME, FALSE from @S3_STAGE (file_format => JSON_FILE_FORMAT, pattern=>'.*ar_details_.*.json');"
#         cursor.execute(insert_query)
#         affected_rows = cursor.fetchall()[0][0]
#         print(f"Ingested data to metadata - {affected_rows}")
#         conn.commit()
#         conn.close()
#         return affected_rows
#     except Exception as e:
#         print(f"error in ingesting ar metadata {str(e)}")
#         if conn:
#             conn.close()
#         raise Exception(str(e))

# def ingest_payment_data_metadata_info(ti):
#     conn = None
#     try:
#         sf_hook = SnowflakeHook(
#             snowflake_conn_id="app_snowflake",
#             database="PROD_DWH",
#             schema="STAGING_SCHEMA",
#         )
#         conn = sf_hook.get_conn()
#         cursor = conn.cursor()
#         # insert_query = f"INSERT INTO PROD_DWH.CURATED_SCHEMA.METADATA_TABLE (FILE_NAME, IS_FLATTENED, FILE_TYPE, LAST_UPDATED_DATE) select  METADATA$FILENAME as FILE_NAME, FALSE, UPPER(split_part(FILE_NAME, '.', -1)) , METADATA$FILE_LAST_MODIFIED as LAST_UPDATED_DATE from @S3_STAGE (file_format => JSON_FILE_FORMAT, pattern=>'.*PAYMENT_DETAILS_.*.json');"

#         insert_query = f"INSERT INTO PROD_AUDIT_DB.AUDIT_SCHEMA.PAYMENTDETAILS_AUDIT (FILENAME, UPLOADTIME, ISFLATTENED) select  METADATA$FILENAME as FILENAME,  METADATA$FILE_LAST_MODIFIED as UPLOADTIME, FALSE from @S3_STAGE (file_format => JSON_FILE_FORMAT, pattern=>'.*ar_payment_.*.json');"
#         cursor.execute(insert_query)
#         affected_rows = cursor.fetchall()[0][0]
#         logger.debug(f"Ingested data to metadata - {affected_rows}")
#         conn.commit()
#         conn.close()
#         return affected_rows
#     except Exception as e:
#         print(f"error in ingesting ar metadata {str(e)}")
#         if conn:
#             conn.close()
#         raise Exception(str(e))

# def ingest_customer_data_metadata_info(ti):
#     conn = None
#     try:
#         sf_hook = SnowflakeHook(
#             snowflake_conn_id="app_snowflake",
#             database="PROD_DWH",
#             schema="STAGING_SCHEMA",
#         )
#         conn = sf_hook.get_conn()
#         cursor = conn.cursor()
#         # insert_query = f"INSERT INTO PROD_DWH.CURATED_SCHEMA.METADATA_TABLE (FILE_NAME, IS_FLATTENED, FILE_TYPE, LAST_UPDATED_DATE) select  METADATA$FILENAME as FILE_NAME, FALSE, UPPER(split_part(FILE_NAME, '.', -1)) , METADATA$FILE_LAST_MODIFIED as LAST_UPDATED_DATE from @S3_STAGE (file_format => JSON_FILE_FORMAT, pattern=>'.*PAYMENT_DETAILS_.*.json');"

#         insert_query = f"INSERT INTO PROD_AUDIT_DB.AUDIT_SCHEMA.CUSTOMERDETAILS_AUDIT (FILENAME, UPLOADTIME, ISFLATTENED) select  METADATA$FILENAME as FILENAME,  METADATA$FILE_LAST_MODIFIED as UPLOADTIME, FALSE from @S3_STAGE (file_format => JSON_FILE_FORMAT, pattern=>'.*ar_payment_.*.json');"
#         cursor.execute(insert_query)
#         affected_rows = cursor.fetchall()[0][0]
#         logger.debug(f"Ingested data to metadata - {affected_rows}")
#         conn.commit()
#         conn.close()
#         return affected_rows
#     except Exception as e:
#         print(f"error in ingesting ar metadata {str(e)}")
#         if conn:
#             conn.close()
#         raise Exception(str(e))
