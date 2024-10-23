# coding=utf-8
import pymysql
import subprocess
import argparse
import yaml
import time
import os


def connect_doris(host="xxx", port=8080, cluster="default_cluster", user=None, password=None):
    if user is None and password is None:
        user = 'root'
        password = '123456'
    host += "-doris.example.com"
    con = pymysql.connect(host=host, port=port, user=user, passwd=password)
    cur = con.cursor()

    # all command must after enter cluster
    #cur.execute('enter %s;' % cluster)
    #cur.fetchall()
    cur.execute("set wait_timeout = 28800")
    cur.fetchall()
    return cur


def create_tables(src_cur, dst_cur, src_db, dst_db, tables=[]):
    src_cur.execute("use %s;" % src_db)
    src_cur.fetchall()

    # if table is not specified, load all tables under src_db
    if len(tables) == 0:
        src_cur.execute("show tables;")
        row_tables = src_cur.fetchall()
        for table in row_tables:
            tables.append(table[0])
    for table_name in tables:
        try:
            src_cur.execute("show create table %s;" % table_name)
            create_sql = src_cur.fetchall()
            # replace replicas
            create_sql = create_sql[0][1].replace('\n"replication_allocation" = "tag.location.default: 3",', '')
            # add "IF NOT EXISTS" to prevent creation failure if table exists
            start_index = create_sql.find("CREATE TABLE") + len("CREATE TABLE")
            create_sql = create_sql[0:start_index] + " IF NOT EXISTS" + create_sql[start_index:]
            if "HLL_UNION" in create_sql:
                create_sql = create_sql.replace("HLL_UNION", "HLL_UNION NOT")
            if "BITMAP_UNION" in create_sql:
                create_sql = create_sql.replace("BITMAP_UNION", "BITMAP_UNION NOT")
            # execute create table query
            dst_cur.execute("use %s" % dst_db)
            dst_cur.execute(create_sql)
            dst_cur.fetchall()
            print("Table %s create success." % table_name)
        except Exception as e:
            print("Table %s create failed. exception: %s" % (table_name, e))


def exec_doris(cur, sql):
    cur.execute(sql)
    res = cur.fetchall()
    return res

def truncate_table(cur, db, table, truncate=False):
    table_full_name = f"""`{db}`.`{table}`"""
    if truncate:
        truncate_sql = "truncate table %s" % table_full_name
        print("Truncate table %s ..." % table_full_name)
        exec_doris(cur, truncate_sql)
    else:
        print("Skip truncate table %s" % table_full_name)

def get_partition_key(cur, db, table):
    exec_doris(cur, ("use %s;" % db))
    create_sql = exec_doris(cur, ("show create table %s;" % table))
    if "PARTITION" not in create_sql:
        return None
    parts = create_sql[0][1].split("PARTITION BY RANGE(`")[1].split("`)")
    partition_key = parts[0] if len(parts) > 0 else None
    print(partition_key)

    return partition_key

def write_file(file_name, content, mode):
    with open(file_name, mode) as file:
        file.write(content)

def generate_insert_sql(src_host, dst_host, db, table, start_date, end_date, load_all, parallel, partition_key):
    src_table = src_host + "." + table
    dst_table = dst_host + "." + table
    spark_options = { "SET spark.sql.catalog.doris_catalog.batch.size=1000",
                      "SET spark.sql.catalog.doris_catalog.deserialize.queue.size=10",
                      "SET spark.sql.catalog.doris_catalog.deserialize.arrow.async=true",
                      "SET spark.sql.shuffle.partitions=20",
                      "SET spark.sql.adaptive.enabled=true"
                    }
    insert_sql = ";\n".join(spark_options)

    insert_sql += f";\n\n/* insert_repartitions({parallel}) */"
    if partition_key is not None and load_all is False:
        insert_sql += f"\nINSERT INTO TABLE `{dst_table}` SELECT * FROM `{src_table}` WHERE {partition_key} BETWEEN {start_date} AND {end_date}"
    else:
        insert_sql += f"\nINSERT INTO TABLE `{dst_table}` SELECT * FROM `{src_table}`"

    sql_file = "load_%s_%s.sql" % (db, table)
    print(insert_sql)
    write_file(sql_file, insert_sql, "w")
    return sql_file

def generate_property_file(src_host, dst_host, src_db, dst_db, table, sql_file):
    properties_list = [
        f"sql.file={sql_file}\n",
        f"doris.jdbc.instance.{dst_host}.url=jdbc:mysql://{dst_host}-doris.example.com:8080/{dst_db}?rewriteBatchedStatements=true",
        f"doris.jdbc.instance.{dst_host}.user=admin",
        f"doris.jdbc.instance.{dst_host}.password=123456\n",
        f"spark.sql.catalog.doris_catalog.instance.{dst_host}.fenodes={dst_host}-doris.example.com:8410",
        f"spark.sql.catalog.doris_catalog.instance.{dst_host}.request.auth.user=admin",
        f"spark.sql.catalog.doris_catalog.instance.{dst_host}.request.auth.password=123456",
        f"spark.sql.catalog.doris_catalog.instance.{dst_host}.default_cluster=default_cluster",
        f"spark.sql.catalog.doris_catalog.instance.{dst_host}.default_db={dst_db}\n",
        f"doris.jdbc.instance.{src_host}.url=jdbc:mysql://{src_host}-doris.example.com:8080/{src_db}?rewriteBatchedStatements=true",
        f"doris.jdbc.instance.{src_host}.user=admin",
        f"doris.jdbc.instance.{src_host}.password=123456\n",
        f"spark.sql.catalog.doris_catalog.instance.{src_host}.fenodes={src_host}-doris.example.com:8410",
        f"spark.sql.catalog.doris_catalog.instance.{src_host}.request.auth.user=admin",
        f"spark.sql.catalog.doris_catalog.instance.{src_host}.request.auth.password=123456",
        f"spark.sql.catalog.doris_catalog.instance.{src_host}.default_cluster=default_cluster",
        f"spark.sql.catalog.doris_catalog.instance.{src_host}.default_db={src_db}"
    ]

    properties = "\n".join(properties_list)
    print(properties)

    properties_file = "config_%s_%s.properties" % (dst_db, table)
    write_file(properties_file, properties, "w")
    return properties_file

def generate_hdfs_login(yarn_file):
    cmds = [
        "source /hadoop_user_login.sh hadoop-launcher",
        "export HADOOP_PROXY_USER=hadoop-user\n"
    ]

    login_cmds = "\n".join(cmds)
    print(login_cmds)

    write_file(yarn_file, login_cmds, "w")

def generate_spark_submit_cmd(sql_file, properties_file, yarn_file):
    cmds = [
        "./bin/spark-submit",
        "--master", "yarn",
        "--deploy-mode", "cluster",
        "--queue", "root.yg.hadoop-olap.etl",
        "--files", f"{sql_file},{properties_file}",
        "--conf", "spark.shuffle.useOldFetchProtocol=true",
        "--conf", "spark.shuffle.service.enabled=true",
        "--conf", "spark.sql.adaptive.enabled=true",
        "--conf", "spark.driver.memory=2G",
        "--conf", "spark.executor.memory=2G",
        "--conf", "spark.sql.adaptive.enabled=true",
        "--conf", f"spark.sparkondoris.sqlfile={sql_file}",
        "--conf", f"spark.sparkondoris.configfile={properties_file}",
        "--conf", "spark.sql.catalog.doris_catalog.deserialize.arrow.async=true",
        "--conf", "spark.dynamicAllocation.enabled=true",
        "--conf", "spark.shuffle.useOldFetchProtocol=true",
        "--conf", "spark.shuffle.service.enabled=true",
        "--conf", "spark.dynamicAllocation.minExecutors=0",
        "--conf", "spark.dynamicAllocation.maxExecutors=500",
        "--conf", "spark.driver.memory=8G",
        "--conf", "spark.executor.memory=12G",
        "--conf", "spark.sql.catalog.doris_catalog.dsn.reads=doris_default_cluster_waimai_detail",
        "--conf", "spark.sql.catalog.doris_catalog.dsn.writes=doris_default_cluster_waimai_detail",
        "sparkondoris-doris2doris-2.5-SNAPSHOT.jar"
    ]

    spark_submit_cmd = " ".join(cmds)
    print(spark_submit_cmd)

    write_file(yarn_file, spark_submit_cmd, "a")

def run_spark_submit(load_job, log_dir, yarn_file):
    if log_dir is not None:
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file = "%s/%s.log" % (log_dir, load_job)
        with open(log_file, 'w') as log:
            subprocess.call(["sh", yarn_file], stdout=log, stderr=log)
    else:
        subprocess.call(["sh", yarn_file])

def run_clean_up(sql_file, properties_file, yarn_file):
    os.remove(sql_file)
    os.remove(properties_file)
    os.remove(yarn_file)


def run_yarn(host, db, table, log_dir, sql_file, properties_file):
    load_job = "load_job_%s_%s_%s" % (host, db, table)
    yarn_file = "run-yarn-%s" % (load_job)
    try:
        print("Start dispatching load job for %s ..." % load_job)
        # hadoop credentials
        generate_hdfs_login(yarn_file)
        # spark submit cmd
        generate_spark_submit_cmd(sql_file, properties_file, yarn_file);

        # run the generated script
        run_spark_submit(load_job, log_dir, yarn_file)

        # clean up intermediate file
        run_clean_up(sql_file, properties_file, yarn_file)
        print("post process for load job %s ..." % load_job)

        print("load job %s succeeded" % (load_job))
    except Exception as e:
        run_clean_up(sql_file, properties_file, yarn_file)
        print("load job %s failed. exception: %s" % (load_job, e))


def run_load_task(src_host, dst_host, src_db, dst_db, start_date, end_date, tables=[], load_all=False, truncate=False, parallel=1, log_dir=None):
    src_cur = connect_doris(host=src_host)
    exec_doris(src_cur, ("use %s;" % src_db))
    if len(tables) == 0:
        src_cur.execute("show tables;")
        db_tables = src_cur.fetchall()
        for table in db_tables:
            tables.append(table[0])

    for table in tables:
        dst_cur = connect_doris(host=dst_host)
        exec_doris(dst_cur, ("use %s;" % dst_db))
        src_cur = connect_doris(host=src_host)
        exec_doris(src_cur, ("use %s;" % src_db))
        try:
            # truncate dst table if necessary
            truncate_table(dst_cur, dst_db, table, truncate)

            # generate insert sql
            partition_key = get_partition_key(src_cur, src_db, table)
            sql_file = generate_insert_sql(src_host, dst_host, dst_db, table, start_date, end_date, load_all, parallel, partition_key);

            # generate properties
            properties_file = generate_property_file(src_host, dst_host, src_db, dst_db, table, sql_file)

            # run spark job
            run_yarn(dst_host, dst_db, table, log_dir, sql_file, properties_file)
        except Exception as e:
            print("Table %s sync data failed! exception: %s." % (table, e))


def parse_args():
    parser = argparse.ArgumentParser(description="Parse command line arguments for Doris audit log.")
    parser.add_argument("config_file", type=str, default="config.yaml", help="Path to the configuration file")
    parser.add_argument("--src_domain", type=str, help="The domain name of src Doris cluster")
    parser.add_argument("--dst_domain", type=str, help="The domain name of src Doris cluster")
    parser.add_argument("--src_db", type=str, help="The db name of src Doris cluster")
    parser.add_argument("--dst_db", type=str, help="The db name of src Doris cluster")
    parser.add_argument("--tables", type=str, help="A comma-separated list of tables")
    parser.add_argument("--start_date", type=int, help="start date of data load")
    parser.add_argument("--end_date", type=int, help="end date of data load")
    parser.add_argument("--load_all", type=bool, help="load all partition data")
    parser.add_argument("--truncate", type=bool, help="truncate table or not")
    parser.add_argument("--parallel", type=int, help="streamload threads number")
    parser.add_argument("--log_dir", type=str, help="directory to store logs")
    args = parser.parse_args()
    return args


def read_config_file(file_path):
    with open(file_path, 'r') as f:
        config = yaml.safe_load(f)
    return config


def get_config():
    args = parse_args()

    config = read_config_file(args.config_file)

    final_config = {
        'src_domain': args.src_domain or config.get('src_domain'),
        'dst_domain': args.dst_domain or config.get('dst_domain'),
        'src_db': args.src_db or config.get('src_db'),
        'dst_db': args.dst_db or config.get('dst_db'),
        'tables': args.tables or config.get('tables'),
        'start_date': args.start_date or config.get('start_date'),
        'end_date': args.end_date or config.get('end_date'),
        'load_all': args.load_all or config.get('load_all'),
        'truncate': args.truncate or config.get('truncate'),
        'parallel': args.parallel or config.get('parallel'),
        'log_dir': args.log_dir or config.get('log_dir'),
    }

    print("config_file: {}".format(args.config_file))
    print("src_domain: {}".format(final_config['src_domain']))
    print("dst_domain: {}".format(final_config['dst_domain']))
    print("src_db: {}".format(final_config['src_db']))
    print("dst_db: {}".format(final_config['dst_db']))
    print("tables: {}".format(final_config['tables']))
    print("start_date: {}".format(final_config['start_date']))
    print("end_date: {}".format(final_config['end_date']))
    print("load_all: {}".format(final_config['load_all']))
    print("truncate: {}".format(final_config['truncate']))
    print("parallel: {}".format(final_config['parallel']))
    print("log_dir: {}".format(final_config['log_dir']))
    print("------------------------------------------------------------")

    if final_config['tables']:
        if "," in final_config['tables']:
            final_config['tables'] = final_config['tables'].split(',')
        else:
            final_config['tables'] = [final_config['tables']]
    else:
        print("tables is empty!")
        final_config['tables'] = []

    return final_config


if __name__ == '__main__':
    config = get_config()
    src_host = str(config['src_domain'])
    dst_host = str(config['dst_domain'])
    src_db = str(config['src_db'])
    dst_db = str(config['dst_db'])
    tables = config['tables']
    start_date = int(config['start_date'])
    end_date = int(config['end_date'])
    load_all = bool(config['load_all'])
    truncate = bool(config['truncate'])
    parallel = int(config['parallel'])
    log_dir = str(config['log_dir'])

    src_cur = connect_doris(host=src_host)
    dst_cur = connect_doris(host=dst_host)

    create_tables(src_cur, dst_cur, src_db, dst_db, tables)

    run_load_task(src_host, dst_host, src_db, dst_db, start_date, end_date, tables, load_all, truncate, parallel, log_dir)
