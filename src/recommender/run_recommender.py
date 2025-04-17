import oracledb
import os
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.append(str(project_root))

# 导入模块
from analysis.satellite_data_extractor import SatelliteDataExtractor
from analysis.order_statistics import OrderAnalyzer

def main():
    # 1. 建立数据库连接
    try:
        # 使用配置信息连接数据库
        conn = oracledb.connect(
            user="jgf_gxfw",
            password="icw3kx45",
            dsn="10.82.8.4:1521/satdb"
        )
        print("数据库连接成功")
    except Exception as e:
        print(f"数据库连接失败: {str(e)}")
        return

    # 2. 初始化分析器
    analyzer = OrderAnalyzer(conn)
    extractor = SatelliteDataExtractor(conn)

    # 3. 获取所有用户
    cursor = conn.cursor()
    try:
        # 查询TF_ORDER表中第一列所有唯一用户
        cursor.execute("""
            SELECT DISTINCT F_LOGIN_USER 
            FROM TF_ORDER 
            WHERE F_LOGIN_USER IS NOT NULL
        """)
        users = [row[0] for row in cursor.fetchall()]
        print(f"找到 {len(users)} 个用户")
    except Exception as e:
        print(f"查询用户失败: {str(e)}")
        return
    finally:
        cursor.close()

    # 4. 创建输出目录
    output_dir = "user_satellite_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"创建输出目录: {output_dir}")

    # 5. 批量处理用户数据
    print("\n开始批量处理用户数据...")
    extractor.batch_process_users(users, output_dir)

    # 6. 生成分析报告
    print("\n生成分析报告...")
    analyzer.print_analysis_report()

    # 7. 关闭数据库连接
    conn.close()
    print("\n处理完成")

if __name__ == "__main__":
    main() 