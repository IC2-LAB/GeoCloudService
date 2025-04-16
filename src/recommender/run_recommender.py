import oracledb
import os
from analysis.satellite_data_extractor import SatelliteDataExtractor
from analysis.order_statistics import OrderAnalyzer

def main():
    # 1. 建立数据库连接
    try:
        conn = oracledb.connect(
            user="your_username",
            password="your_password",
            dsn="your_dsn"
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