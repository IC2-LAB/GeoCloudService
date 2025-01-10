import subprocess
import os
from pathlib import Path
import time
from datetime import timedelta
import traceback
import oracledb

import src.config.config as config
from src.utils.logger import logger, file_handler
from src.geocloudservice.recommend import generateSqlQuery

file_handler.setLevel("WARNING")
logger.addHandler(file_handler)

BATCH_SIZE = 1000
CURRENT_FILE_DIR = Path(__file__).resolve().parent()
JAR_PATH = CURRENT_FILE_DIR.joinpath("Example","RBGdal.jar")
JAR_CWD = CURRENT_FILE_DIR.joinpath("Example")
TABLE_LIST = list(config.NodeIdToNodeName.values())

WHERE_SQL = """ F_SHAPEIMAGE IS NULL 
            AND F_THUMIMAGE IS NOT NULL 
            AND DBMS_LOB.GETLENGTH(F_THUMIMAGE) > 0 
            AND F_SHAPE_DEL IS NULL """


def get_null_shape_images_batch(cursor, batch_size=1000):
    """
    获取所有未生成shapeimage的图片
    生成器，每次返回batch_size条数据
    """
    offset = 0
    while True:
        cursor.execute(
            f"""
            SELECT F_DID, F_THUMIMAGE 
            FROM TB_BAS_META_BLOB 
            WHERE {WHERE_SQL}
            OFFSET :offset ROWS FETCH NEXT :batch_size ROWS ONLY
            """,
            {"offset": offset, "batch_size": batch_size},
        )
        results = cursor.fetchall()
        if len(results) == 0:
            break
        yield results
        offset += batch_size

def get_null_shape_images_count(cursor):
    cursor.execute(f"SELECT COUNT(*) FROM TB_BAS_META_BLOB WHERE {WHERE_SQL}")
    return cursor.fetchone()[0]

def get_shapeimage_position(did, cursor):
    data_name = [
        "f_topleftlongitude",
        "f_topleftlatitude",
        "f_bottomleftlongitude",
        "f_bottomleftlatitude",
        "f_toprightlongitude",
        "f_toprightlatitude",
        "f_bottomrightlongitude",
        "f_bottomrightlatitude",
    ]
    where_sql = f"where f_did = :did"
    sql = generateSqlQuery(data_name, TABLE_LIST, where_sql)

    try:
        cursor.execute(sql, did=did)
        res = cursor.fetchone()
        if res is None:
            logger.error(f"did: {did}, no position data")
            return None  # 一般是航片图
        columns = [col[0] for col in cursor.description]
        dict_res = dict(zip(columns, map(str, res)))
        logger.info(f"did: {did}, get position successfully")
        return dict_res
    except Exception as e:
        logger.error(f"did: {did}, get position error: {e}")
        return None


def get_shapeimage(did, input_path: Path, output_path: Path, pos: dict):
    input_path, output_path = input_path.resolve(), output_path.resolve()
    jar_st_time = time.perf_counter()
    try:
        result = subprocess.run(
            [
                "java",
                "-jar",
                JAR_PATH,
                str(input_path),
                *pos.values(),
                str(output_path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        if result.stderr:
            raise subprocess.CalledProcessError(
                result.returncode, result.args, result.stderr
            )

    except subprocess.CalledProcessError as e:
        if result.stderr:
            err = result.stderr.strip()
        elif result.stdout:
            err = result.stdout.strip()
        else:
            err = e
        logger.error(f"did: {did}, java execution error: {err}")
        return None

    logger.info(f"did: {did}, shapeimage generated, cost {time.perf_counter() - jar_st_time:.2f}s")
    try:
        with open(output_path, "rb") as file:
            shapeimage_data = file.read()
        return shapeimage_data
    except OSError as e:
        logger.error(f"did: {did}, read shapeimage error: {e}")
        return None


def add_shape_del(did, proc_cur, message=""):
    try:
        proc_cur.execute(
            "UPDATE TB_BAS_META_BLOB SET F_SHAPE_DEL = 1 WHERE F_DID = :1", [did]
        )
        logger.info(f"did: {did} F_SHAPE_DEL set to 1")
    except Exception as e:
        logger.error(f"did: {did}, update shape_del error: {e}")


def add_blob(did, cursor, shapeimage_data):
    try:
        cursor.execute(
            "UPDATE TB_BAS_META_BLOB SET F_SHAPEIMAGE = :1 WHERE F_DID = :2",
            [shapeimage_data, did],
        )
        logger.info(f"did: {did}, shapeimage updated")
    except Exception as e:
        logger.error(f"did: {did}, update shapeimage error: {e}")


def create_dbconn():
    username = config.DB_USER
    password = str(config.DB_PWD)
    host = config.DB_HOST
    port = str(config.DB_PORT)
    database = config.DB_DATABASE
    conn_str = username + "/" + password + "@" + host + ":" + port + "/" + database
    conn = oracledb.connect(conn_str)
    return conn


def process_batch(batch, conn):
    proc_cur = conn.cursor()
    try:
        for row in batch:
            did, thumimage_data = row
            logger.info(f"get did: {did}")

            pos = get_shapeimage_position(did, proc_cur)
            if pos is None:
                add_shape_del(did, proc_cur, message="获取位置信息错误")
                continue

            temp_image_path = Path(f"./tmp/{did}_thumbimage.jpeg")
            with open(temp_image_path, "wb") as file:
                file.write(thumimage_data.read())

            output_image_path = Path(f"./tmp/{did}_shapeimage.png")
            if output_image_path.exists():
                output_image_path.unlink()

            shapeimage_data = get_shapeimage(
                did, temp_image_path, output_image_path, pos
            )
            if shapeimage_data is None:
                add_shape_del(did, proc_cur, message="配准过程出错")
                temp_image_path.unlink()
                try:
                    output_image_path.unlink()
                except FileNotFoundError:
                    pass
                continue

            add_blob(did, proc_cur, shapeimage_data)
            temp_image_path.unlink()
            output_image_path.unlink()

        # 处理完一个批次后提交
        conn.commit()
        logger.info("[process_batch] commit successfully! **** ")
    except Exception as e:
        conn.rollback()
        logger.error(
            f"[process_batch] error: {type(e).__name__} {e}\ntraceback: {traceback.format_exc()}"
        )
        raise
    finally:
        proc_cur.close()


def main():
    main_st_time = time.time()
    os.chdir(JAR_CWD)
    Path("./tmp").mkdir(parents=True, exist_ok=True)

    conn = create_dbconn()
    query_cur = conn.cursor()
    try:
        logger.warning(f"[main] total count: {get_null_shape_images_count(query_cur)}")
        gen = get_null_shape_images_batch(query_cur, BATCH_SIZE)
        while True:
            process_batch(next(gen), conn)
    except StopIteration:
        logger.info(f"[main] all done")

    except Exception as e:
        logger.error(
            f"[main] error: {type(e).__name__} {e}\ntraceback: {traceback.format_exc()}"
        )
    finally:
        query_cur.close()
        conn.close()
        logger.info(f"[main] cost {timedelta(seconds=time.time() - main_st_time)}")


def main_schedule():
    import schedule
    schedule.every().day.at("03:00").do(main)
    while True:
        schedule.run_pending()
        time.sleep(60)
    
    
if __name__ == "__main__":
    main()
