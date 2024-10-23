import subprocess
import argparse
import json
import re
import yaml

def parse_args():
    parser = argparse.ArgumentParser(description="Parse command line arguments for Doris audit log.")
    parser.add_argument("config_file", type=str, default="config.yaml", help="Path to the configuration file")
    parser.add_argument("--domain", type=str, help="The domain name of Doris cluster")
    parser.add_argument("--db", type=str, help="Database name")
    parser.add_argument("--tables", type=str, help="A comma-separated list of tables")
    parser.add_argument("--date", type=str, help="Partition date of audit log")
    parser.add_argument("--limit", type=int, help="Number of audit log records queried")
    parser.add_argument("--output_file", type=str, help="File to redirect output")
    args = parser.parse_args()
    return args

def read_config_file(file_path):
    with open(file_path, 'r') as f:
        config = yaml.safe_load(f)
    #print("config:{}".format(config))
    return config

def get_config():
    args = parse_args()

    config = read_config_file(args.config_file)

    final_config = {
        'domain': args.domain or config.get('domain'),
        'db': args.db or config.get('db'),
        'tables': args.tables or config.get('tables'),
        'date': args.date or config.get('date'),
        'limit': args.limit or config.get('limit'),
        'output_file': args.output_file or config.get('output_file')
    }

    print("config_file: {}".format(args.config_file))
    print("domain: {}".format(final_config['domain']))
    print("database: {}".format(final_config['db']))
    print("tables: {}".format(final_config['tables']))
    print("date: {}".format(final_config['date']))
    print("limit: {}".format(final_config['limit']))
    print("output_file: {}".format(final_config['output_file']))
    print("------------------------------------------------------------")

    final_config['tables'] = final_config['tables'].split(',')

    return final_config

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
        #print(formatted_json_str)
        return formatted_json_str
    except ValueError as e:
        print("Failed to process JSON: ", e)
        return None

def write_to_file(output_file, content):
    print("------------------------------------------------------------")
    print("Write result to output file: {}".format(output_file))
    with open(output_file, 'w') as f:
        f.write(content.encode('utf-8'))

def main():
    config = get_config()
    sql = generate_sql_command(config['domain'], config['db'], config['tables'], config['date'], config['limit'])
    output = run_presto_command(sql)
    if output and len(output) > 3:
        formatted_output = process_json_output(output)
        if formatted_output:
            write_to_file(config['output_file'], formatted_output)
    else:
        print("------------------------------------------------------------")
        print("No queries found in audit log, please change filters...")

if __name__ == "__main__":
    main()
