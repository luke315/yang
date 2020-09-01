


def select_sql_func(conn_db, table, fields, where_fields):
    cursor = conn_db.cursor()
    sql = f"""select {fields} from {table} where {where_fields}"""
    cursor.execute(sql)
    result1 = cursor.fetchall()
    return result1

def insert_sql_func(conn_db, table, fields, where_fields):
    cursor = conn_db.cursor()
    sql = f"""insert into {table} ({where_fields}) values({fields})"""
    try:
        cursor.execute(sql)
        conn_db.commit()
    except Exception as e:
        print(e)
        conn_db.rollback()

def update_sql_func(conn_db, table, fields, where_fields):
    cursor = conn_db.cursor()
    sql = f"""update {table} set {fields} where {where_fields}"""
    try:
        cursor.execute(sql)
        conn_db.commit()
    except Exception as e:
        print(e)
        conn_db.rollback()