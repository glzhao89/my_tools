import subprocess
import argparse
import json
import re

def parse_args():
    parser = argparse.ArgumentParser(description="Parse command line arguments for Doris audit log.")
    parser.add_argument("domain", type=str, help="The domain name of Doris cluster")
    parser.add_argument("db", type=str, help="Database name")
    parser.add_argument("tables", type=str, help="A comma-separated list of tables")
    parser.add_argument("date", type=str, help="Partition date of audit log")
    parser.add_argument("limit", type=int, help="Number of audit log records queried")
    parser.add_argument("output_file", type=str, help="File to redirect output")
    args = parser.parse_args()

    domain = args.domain
    db = args.db
    date = args.date
    tables = args.tables.split(',')
    limit = args.limit
    output_file = args.output_file

    print("domain: {}".format(args.domain))
    print("database: {}".format(args.db))
    print("tables: {}".format(args.tables))
    print("date: {}".format(args.date))
    print("limit: {}".format(args.limit))
    print("output_file: {}".format(args.output_file))
    print("------------------------------------------------------------")

    return domain, db, tables, date, limit, output_file

def generate_sql_command(domain, db, tables, date, limit):
    tables_sql = ', '.join(["'{}'".format(table) for table in tables])
    sql_query = """
    SELECT
        CAST(map_agg(query_id, sql_str) AS JSON)
    FROM (
        SELECT query_id, URL_DECODE(sql) AS sql_str
        FROM log.data_doris_audit
        WHERE dt = {}
          AND domain = '{}'
          AND is_query = 1
          AND success = 1
          AND db = '{}'
          AND scan_tables <> ''
          AND all_match(SPLIT(scan_tables, ','), e -> e IN ({}))
        LIMIT {}
    );
    """.format(date, domain, db, tables_sql, limit)
    return sql_query

def run_presto_command(sql_command):
    command = "presto --execute \"{}\"".format(sql_command)
    print(command)
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode == 0:
        return stdout
    else:
        print("Command failed with error:")
        print(stderr)
        return None

def process_json_output(json_str):
    if json_str.endswith('\n'):
        json_str = json_str[:-1]
    if json_str.startswith('"') and json_str.endswith('"'):
        json_str = json_str[1:-1]

    json_str = re.sub(r'""([^""]+)""', r'"\1"', json_str)

    try:
        data = json.loads(json_str)
        print("JSON is valid.")
        formatted_json_str = json.dumps(data, indent=4, ensure_ascii=False)
        print(formatted_json_str)
        return formatted_json_str
    except ValueError as e:
        print("Failed to process JSON: ", e)
        return None

def write_to_file(output_file, content):
    with open(output_file, 'w') as f:
        f.write(content.encode('utf-8'))

def main():
    domain, db, tables, date, limit, output_file = parse_args()
    sql = generate_sql_command(domain, db, tables, date, limit)
    output = run_presto_command(sql)
    if output:
        formatted_output = process_json_output(output)
        if formatted_output:
            write_to_file(output_file, formatted_output)


if __name__ == "__main__":
    main()
