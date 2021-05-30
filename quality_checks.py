import configparser
import psycopg2
from sql_queries import quality_check_definitions


def perform_quality_checks(cur, conn):
    errors = []
    for quality_checks in quality_check_definitions:
        for name, query in quality_checks.items():
            print(name, query)
            cur.execute(query)
            rows_returned = len(cur.fetchall())
            conn.commit()
            if rows_returned > 0:
                errors.append((name, query, rows_returned))
    if len(errors) == 0:
        print("SUCCESS: All checks passed.")
    else:
        for error in errors:
            print(
                f"ERROR: Quality check {error[0]} got {error[2]} results for query {error[1]}"
            )


def main():
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()

    perform_quality_checks(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
