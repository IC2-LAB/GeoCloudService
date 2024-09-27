import schedule

from src.data_extraction_service.external.schedule import orderProcess


def main():
    schedule.every(30).minutes.do(orderProcess.writeOrderData)
    schedule.every(40).minutes.do(orderProcess.readOrderData)
