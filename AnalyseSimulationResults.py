"""
So we've simulated a load of traffic, now let's have a look at the results...
"""

import psycopg2
import yaml
import matplotlib.ticker as mtick
import pandas as pd
import matplotlib.pyplot as plt
import seaborn
import logging
import argparse


def GetdecileAggregates(db, jobs):
    """
    Gets the decile aggregates from the DB
    :param db:
    :return:
    """
    cursor = db.cursor()
    query = """
    select decile,
       rate,
       sum(r.actual_bytes) as actual_bytes,
       sum(r.actual_packets) as actual_packets,
       sum(r.sampled_bytes) as sampled_bytes,
       sum(r.sampled_packets) as sampled_packets,
       (sum(r.actual_bytes) - sum(r.sampled_bytes)*rate)/sum(r.actual_bytes) as error
from (select sum(a.bytes)                           as actual_bytes,
             sum(a.packets)                         as actual_packets,
             s.rate                                 as rate,
             sum(s.packets)                         as sampled_packets,
             sum(s.bytes)                           as sampled_bytes,
             ntile(10) over (order by sum(a.bytes)) as decile
      from actor a,
           sample s
      where s.actor_id = a.id
        and a.job_id between %s and %s
      group by a.name, s.rate) r
group by rate, decile
order by rate desc, decile
    """

    cursor.execute(query, [min(jobs), max(jobs)])
    records = [{"decile": decile, "rate": rate, "actual_bytes": actual_bytes, "actual_packets": actual_packets,
                "sampled_bytes": sampled_bytes, "sampled_packets": sampled_packets, "error": error}
        for decile, rate, actual_bytes, actual_packets, sampled_bytes, sampled_packets, error in cursor.fetchall()]
    cursor.close()
    return pd.DataFrame(records)


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description="Analyse sFlow data and then simulate it")
    ap.add_argument("--debug", action="store_true", help="Debug mode")
    ap.add_argument("--credentials", metavar="YAML file", help="File containing DB credentials",
                    default="credentials.yaml")
    ap.add_argument("--flows", metavar="JSON file", help="Flow data")
    ap.add_argument("--jobs", help="JobIDs of interest", nargs="+", action="append", type=int,
                    default=range(99, 119))
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")
    config = yaml.load(open(args.credentials), yaml.FullLoader)

    db = psycopg2.connect(**config)
    aggs = GetdecileAggregates(db, args.jobs)
    db.close()
    seaborn.set()
    error_rates = aggs.pivot(index="decile", columns="rate")["error"]
    styles = ['rs:', 'bo:', 'gv:', 'c>:', 'm<:', 'y^:', 'kx:']
    ax = error_rates.plot(style=styles)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    plt.ylabel("Error")
    plt.xlabel("decile")
    plt.title("1Tn packets across 1,000 IP addresses")
    plt.savefig("decile1e12.png")

