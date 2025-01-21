import subprocess
import os
import sys
from pathlib import Path
import time
from datetime import timedelta
import traceback
import oracledb

import src.config.config as config
from src.utils.logger import logger, file_handler
from src.geocloudservice.recommend import generateSqlQuery
from src.shapeimage_generation_script.network_environment import NetworkEnvironment
from src.shapeimage_generation_script.network_environment import NET_ENV

file_handler.setLevel("WARNING")
logger.addHandler(file_handler)

BATCH_SIZE = 5000
CURRENT_FILE_DIR = Path(__file__).resolve().parent
JAR_PATH = CURRENT_FILE_DIR.joinpath("Example", "RBGdal.jar")
JAR_CWD = CURRENT_FILE_DIR.joinpath("Example")
TABLE_LIST = list(config.NodeIdToNodeName.values())
TABLE_LIST.extend(["TB_META_GF1" + c for c in ["B", "C", "D"]])

WHERE_SQL = """
    b.F_SHAPEIMAGE IS NULL
    AND b.F_THUMIMAGE IS NOT NULL
    AND DBMS_LOB.GETLENGTH(b.F_THUMIMAGE) > 100
    AND b.F_SHAPE_DEL IS NULL
    """
WHERE_SQL_INTER = WHERE_SQL.replace("b.F_THUMIMAGE", "t.F_THUMIMAGE")


def get_null_shape_images_batch(cursor, batch_size=1000):
    """
    获取未生成shapeimage的记录
    每次查询batch_size条数据，返回生成器
    did 为可选参数, 用于单独处理某一条数据(测试用)
    """
    if NET_ENV == NetworkEnvironment.EXTERNAL:
        sql = f"""
            SELECT F_DID, F_THUMIMAGE
            FROM TB_BAS_META_BLOB b
            WHERE {WHERE_SQL}
            FETCH NEXT :batch_size ROWS ONLY
            """
    else:
        sql = f"""
            SELECT t.F_DID, t.F_THUMIMAGE
            FROM VIEW_META_BLOB t
            LEFT JOIN TB_BAS_META_BLOB b ON t.F_DID = b.F_DID
            WHERE {WHERE_SQL_INTER}
            FETCH NEXT :batch_size ROWS ONLY
            """
    cursor.execute(sql, {"batch_size": batch_size})
    if cursor.rowcount == 0:
        return None

    while True:
        result = cursor.fetchone()
        if result is None:
            break
        yield result


def get_null_shape_images_count(cursor):
    if NET_ENV == NetworkEnvironment.EXTERNAL:
        cursor.execute(f"SELECT COUNT(*) FROM TB_BAS_META_BLOB b WHERE {WHERE_SQL}")
    else:
        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM VIEW_META_BLOB t
            LEFT JOIN TB_BAS_META_BLOB b ON t.F_DID = b.F_DID
            WHERE {WHERE_SQL_INTER}"""
        )
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
            # 没有此DID的位置信息
            # 可能是 [ 任何表都没有 | TB_META_HANGPIAN航片图 | TB_MATE_YIBANTU一般图? ]
            logger.error(f"did: {did}, no position data")
            return None
        if any([x is None for x in res]):
            # 位置信息有缺失（一般是全空）
            logger.error(f"did: {did}, position data is None")
            return None
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
        if "failures" in result.stdout:
            raise subprocess.CalledProcessError(
                result.returncode, result.args, result.stdout
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

    logger.info(
        f"did: {did}, shapeimage generated, cost {time.perf_counter() - jar_st_time:.2f}s"
    )
    try:
        with open(output_path, "rb") as file:
            shapeimage_data = file.read()
        return shapeimage_data
    except OSError as e:
        logger.error(f"did: {did}, read shapeimage error: {e}")
        return None


def add_shape_del(did, cursor, message=""):
    try:
        cursor.execute(
            "UPDATE TB_BAS_META_BLOB SET F_SHAPE_DEL = 1 WHERE F_DID = :1", [did]
        )
        if cursor.rowcount == 0:
            cursor.execute(
                "INSERT INTO TB_BAS_META_BLOB (F_DID, F_SHAPE_DEL, F_SHAPE_DEL_MESSAGE) VALUES (:1, 1, :2)",
                [did, message],
            )
            logger.warning(f"did: {did}, not in table, F_SHAPE_DEL inserted")
        else:
            logger.info(f"did: {did} F_SHAPE_DEL set to 1")
    except Exception as e:
        logger.error(f"did: {did}, update shape_del error: {e}")


def add_blob(did, cursor, shapeimage_data):
    try:
        cursor.execute(
            "UPDATE TB_BAS_META_BLOB SET F_SHAPEIMAGE = :1 WHERE F_DID = :2",
            [shapeimage_data, did],
        )
        if cursor.rowcount == 0:
            cursor.execute(
                "INSERT INTO TB_BAS_META_BLOB (F_DID, F_SHAPEIMAGE) VALUES (:1, :2)",
                [did, shapeimage_data],
            )
            logger.warning(f"did: {did}, not in table, shapeimage inserted")
        else:
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
    try:
        proc_cur = conn.cursor()
        for row in batch:
            did, thumimage_data = row
            logger.info(f"did: {did}, processing...")

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
            conn.commit()

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
    os.system("del /Q ./tmp/* 2>nul")

    conn = create_dbconn()
    query_cur = conn.cursor()
    try:
        logger.warning(f"[main] total count: {get_null_shape_images_count(query_cur)}")
        while True:
            batch = get_null_shape_images_batch(query_cur, BATCH_SIZE)
            if batch is None:
                break
            process_batch(batch, conn)
        logger.info(f"[main] all done")

    except Exception as e:
        logger.error(
            f"[main] error: {type(e).__name__} {e}\ntraceback: {traceback.format_exc()}"
        )

    except KeyboardInterrupt:
        logger.warning(f"[main] interrupted by keyboard")
        conn.commit()

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
    if len(sys.argv) > 1:
        # 直接运行脚本时，如果有参数，则只处理该单条数据
        did = sys.argv[1]
        WHERE_SQL = f"b.F_DID = {did}"
        WHERE_SQL_INTER = f"t.F_DID = {did}"
    main()
