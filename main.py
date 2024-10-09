import argparse
import commands

parser = argparse.ArgumentParser(prog='GeoCloud Backend Service', description='A backend service for managing geospatial data and services')

subparsers = parser.add_subparsers(title="Data Extraction", dest='subparsers')
#data_extraction_web_subparsers = parser.add_subparsers(title="Data Extraction External", dest='data_extraction_web', help='')
data_extraction_internal_command = subparsers.add_parser("internal", help="Data Extraction Service in Internal Machines")
data_extraction_external_command = subparsers.add_parser("external", help="Data Extraction Service in External Machines")

main_service_command = subparsers.add_parser("web", help="Main Web Service")

args = parser.parse_args()
match args.subparsers:
    case 'internal':
        commands.data_extraction_internal()
    case 'external':
        commands.data_extraction_external()
    case 'web':
        commands.run_web()
    case _:
        print("Invalid command")
