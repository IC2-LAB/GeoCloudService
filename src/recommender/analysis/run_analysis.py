import oracledb
from order_statistics import OrderAnalyzer

def main():
    # 连接数据库
    conn = oracledb.connect(
        user="jgf_gxfw",
        password="icw3kx45",
        dsn="10.82.8.4:1521/satdb"
    )
    
    try:
        # 创建分析器
        analyzer = OrderAnalyzer(conn)
        
        # 运行分析并打印报告
        analyzer.print_analysis_report()
        
    finally:
        # 关闭数据库连接
        conn.close()

if __name__ == "__main__":
    main()