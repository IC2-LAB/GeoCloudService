from flask import request, jsonify, Blueprint, g, current_app
import json

from src.utils.db.oracle import create_pool, executeQueryAsDict
from src.config.config import ENABLE_SM4_ENCRYPTION
from src.utils.sm4encry import SM4Util


def app_get_areas_api(app, siwa):
    get_areas_bp = Blueprint("get_areas", __name__, url_prefix="/agrsArea")

    # 获取所有地区树形结构接口
    @get_areas_bp.route("/get", methods=["GET"])
    @siwa.doc(summary="获取所有地区树形结构接口", description="")
    def get_areas():
        # 解析GET参数
        # GET http://gf.agrs.cn:443/mj/agrsArea/get?showWkt=false&code=000000&qType=0&showType=0&showSub=true&showAllSub=true
        code = request.args.get("code", default="000000")
        q_type = request.args.get("qType", default=0)
        show_wkt = request.args.get("showWkt", default=False)
        show_sub = request.args.get("showSub", default=False)
        show_all_sub = request.args.get("showAllSub", default=False)

        tree = get_ares_tree(code, q_type, show_wkt, show_sub, show_all_sub)
        if not tree:
            return app_response({"error": "未找到对应的地区信息"}, 404)
        return app_response(tree)

    def get_ares_tree(code, q_type, show_wkt, show_sub, show_all_sub):  # 参数均未使用
        pool = g.MyPool
        sql = "SELECT f_name AS name,f_distcode AS code FROM tc_district"
        result = executeQueryAsDict(pool, sql)
        if not result:
            return None
        return build_tree(result)

    def build_tree(data: list[dict]):
        """
        将线性行政区划数据转换为树形结构
        :param data: 行政区数据列表，线性，各个元素是包含code和name字段的字典
        :return: 树形结构数据
        """

        data = [
            {
                "code": item["CODE"].removeprefix("156"),
                "name": item["NAME"],
            }  # removeprefix 需要Python 3.9
            for item in data
        ]
        data.sort(key=lambda x: x["code"])
        if data[0]["code"] == "000000":  # 去除全国'000000'节点避免问题
            data.pop(0)

        nodes = {
            item["code"]: (
                {"code": item["code"], "name": item["name"], "child": []}
                # if item["code"].endswith("00")
                # else {"code": item["code"], "name": item["name"]}
            )
            for item in data
        }

        # 连接父子关系
        municipalities = ["11", "12", "31", "50"]  # 直辖市编码列表
        prov_list = []
        for item in data:
            area_code = item["code"]
            prov_code = area_code[:2] + "0000"  # 获取省级节点代码
            if area_code == prov_code:
                # 省级节点
                prov_list.append(nodes[area_code])
            else:
                city_code = area_code[:4] + "00"  # 获取市级节点代码
                is_county_direct = area_code[2:4] == "90"  # 是否为省直辖县级行政区划
                is_municipality = area_code[:2] in municipalities  # 是否为直辖市
                if area_code == city_code or is_county_direct or is_municipality:
                    # 市级节点
                    nodes[prov_code]["child"].append(nodes[area_code])
                else:
                    # 县级节点
                    if city_code not in nodes:
                        # print(f"未找到{item}的父节点{city_code}")
                        continue
                    nodes[city_code]["child"].append(nodes[area_code])

        return [{"code": "000000", "name": "全国", "child": prov_list}]

    return get_areas_bp


# app端响应通用格式
def app_response(data: dict, status_code: int = 200):
    VERSION = "v0.1.0-bupt"
    response_template = {
        "decryptFlag": ENABLE_SM4_ENCRYPTION,
        "status": status_code,
        "version": VERSION,
    }

    try:
        if response_template["decryptFlag"] and response_template["status"] == 200:
            response_template["data"] = encrypt_data(data)
        else:
            response_template["data"] = data
        return jsonify(response_template), status_code

    except RuntimeError as e:
        error_response = {
            "error": str(e),
            "decryptFlag": False,
            "status": 500,
            "version": VERSION,
        }
        return jsonify(error_response), 500


def encrypt_data(data: dict) -> str:
    """
    帮助函数，用于使用SM4Util加密数据。
    返回加密后的字符串，或在加密失败时引发异常。

    """
    sm4_util: SM4Util = current_app.extensions.get("sm4_util")
    if sm4_util is None:
        raise RuntimeError("SM4模块未初始化")

    try:
        serialized_data = json.dumps(data, ensure_ascii=False)
        encrypted = sm4_util.encrypt_ecb_base64(serialized_data)
        if encrypted is None:
            raise RuntimeError("加密失败")
        return encrypted
    except Exception as e:
        raise RuntimeError(f"加密数据失败：{e}")
