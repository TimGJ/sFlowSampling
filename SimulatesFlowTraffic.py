import pandas as pd
import numpy as np
import collections
import scipy.stats
import argparse
import logging
import math
import yaml
import psycopg2
import enum
import multiprocessing


class Level(enum.Enum):
    """
    Message severites for the logger
    """
    Critical = 5
    Error = 4
    Warning = 3
    Info = 2
    Debug = 1

    def __ge__(self, other):
        if isinstance(other, Level):
            return self.value >= other.value
        else:
            raise TypeError(f"Can't compare class Level with {type(other)}")

class Messenger:
    """
    Class which writes messages to the DB (as we can't necessarily log)
    """
    def __init__(self, db, job_id, run, level=Level.Info):
        self.job_id = job_id
        self.db = db
        self.cursor = db.cursor()
        self.run = run
        self.level = level

    def Critical(self, message):
        self.__write(Level.Critical, message)
        logging.critical(f"JobID={self.job_id} Run={self.run} {message}")
        
    def Error(self, message):
        self.__write(Level.Error, message)
        if self.level >= Level.Error:
            logging.error(f"JobID={self.job_id} Run={self.run} {message}")

    def Warning(self, message):
        self.__write(Level.Warning, message)
        if self.level >= Level.Warning:
            logging.warning(f"JobID={self.job_id} Run={self.run} {message}")

    def Info(self, message):
        self.__write(Level.Info, message)
        if self.level >= Level.Info:
            logging.info(f"JobID={self.job_id} Run={self.run} {message}")

    def Debug(self, message):
        self.__write(Level.Debug, message)
        if self.level >= Level.Debug:
            logging.debug(f"JobID={self.job_id} Run={self.run} {message}")

    def __write(self, level, message):
        cursor = self.db.cursor()
        sql = "INSERT INTO message (job_id, severity, run, message) VALUES (%s, %s, %s, %s)"
        cursor.execute(sql, [self.job_id, level.value, self.run, message])
        self.db.commit()
        cursor.close()



class Counter:
    """
    Very simple class to keep track of number of packets and bytes
    """
    def __init__(self):
        self.bytes = 0
        self.packets = 0

    def __repr__(self):
        return f"{self.bytes:,} bytes ({self.packets:,} packets)"

    def Count(self, bytes):
        self.bytes += bytes
        self.packets += 1

    def Reset(self):
        self.bytes = 0
        self.packets = 0

class IPCounters:
    """
    A class to hold the counts for an individual IP address
    """
    def __init__(self, parent, ip):
        """
        :param parent: The parent Sampler object
        :param ip: The ip address (string)
        """
        self.ip = ip
        self.counter = Counter() # The canonical value
        self.bins = {rate: Counter() for rate in parent.rates}

    def ComputeErrors(self, cursor, job_id):
        asql = "INSERT INTO actor (job_id, name, bytes, packets) VALUES (%s, %s, %s, %s) RETURNING id"
        bsql = "INSERT INTO sample (actor_id, rate, bytes, packets, error) VALUES (%s, %s, %s, %s, %s)"

        cursor.execute(asql, [job_id, self.ip, self.counter.bytes, self.counter.packets])
        actor = cursor.fetchone()[0]
        for rate in self.bins:
            bin = self.bins[rate]
            try:
                error = (bin.bytes * rate - self.counter.bytes)/self.counter.bytes
            except ZeroDivisionError:
                error = None
            else:
                cursor.execute(bsql, [actor, rate, bin.bytes, bin.packets, error])

class Sampler:
    """
    Class to handle the sampling simulation
    """
    def __init__(self, arg, run, ivh, ipmap):
        self.block_size = arg.block_size
        self.block_count = arg.block_count
        self.run = run
        self.flows = arg.flows
        self.rates = arg.rates
        self.bins = arg.bins
        self.ip_pool = arg.ip_pool
        self.db = self.ConnectToDB(arg.credentials)
        self.job_id = self.CreateJob()
        self.messenger = Messenger(self.db, self.job_id, self.run, level=Level.Debug if arg.debug else Level.Info)
        self.ivh = ivh
        self.ipmap = ipmap
        self.messenger.Info(f"Creating {len(self.ipmap):,} IP Counters")
        self.counters = {}
        self.completed = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.messenger:
            self.messenger.Info("Job completed")
            self.messenger.cursor.close()
        if self.db:
            sql = "UPDATE job SET finished=current_timestamp, completed=%s"
            cursor = self.db.cursor()
            cursor.execute(sql, [self.completed])
            cursor.close()
            self.db.commit()
            self.db.close()

    def ConnectToDB(self, credentials):
        """
        Reads the credentials from the file and use them to connect to the DB
        :param credentials: YAML file
        :return: psycopg2 connection
        """
        credentials = yaml.load(open(credentials), Loader=yaml.FullLoader)
        return psycopg2.connect(**credentials)

    def CreateJob(self):
        """
        Creates an entry in the job table and gets the job id
        :return: job id (from database)
        """
        cursor = self.db.cursor()
        sql = "INSERT INTO job (block_size, block_count, ip_pool, histogram_bins) VALUES (%s, %s, %s, %s) RETURNING id"
        cursor.execute(sql, [self.block_size, self.block_count, self.ip_pool, self.bins])
        job_id = cursor.fetchone()[0]
        cursor.close()
        self.db.commit()
        return job_id

    def Sizes(self):
        """
        Generates a set of `block_count` blocks of `block_size` random numbers using `generator` (a
        scipy.stats inverse histogram
        :param generator: scipy.stats inverse histogram
        :param block_size: size of block to yield
        :param block_count: number of passes
        :return: block of packet sizes
        """
        for _ in range(self.block_count):
            yield np.round(self.ivh.rvs(size=self.block_size))

    def IPs(self):
        """

        :param mapping:
        :param block_size:
        :param block_count:
        :return: block of ip addresses (list)
        """
        addresses = list(self.ipmap.keys())
        total = sum(self.ipmap.values())
        prob = [x/total for x in self.ipmap.values()]
        for _ in range(self.block_count):
            yield np.random.choice(addresses, size=self.block_size, replace=True, p=prob)


    def Simulate(self):
        """
        Performs a simulation run. Creates a self.block_count sets of ip addresses and packet lengths,
        eacf of length self.block_size. Then samples updates the counters for all the ip addresses with the
        byte counts (so we know the true value). Ten updates the sample counters for the various different
        sampling rates.
        :return:
        """
        # Reset any counters from previous simulations
        self.messenger.Debug("Resetting counters")
        for counter in self.counters.values():
            counter.Reset()

        for block, (ips, sizes) in enumerate(zip(self.IPs(), self.Sizes()), 1):
            self.messenger.Debug(f"Processing block {block:,} of {self.block_count:,}")
            # First of all update the overall counters
            for ip, size in zip(ips, sizes):
                if ip not in self.counters:
                    self.counters[ip] = IPCounters(self, ip)
                self.counters[ip].counter.Count(size)

            # Now for each sampling rate, update the appropriate sample counters
            for rate in self.rates:
                sample_ips = ips[::rate]
                sample_sizes = sizes[::rate]
                for ip, size in zip(sample_ips, sample_sizes):
                    self.counters[ip].bins[rate].Count(size)

    def AnalyseResults(self):
        """
        Once a simulation is complete, we must analyse the results to see how far out of whack the various
        sampling rates have been.
        :return:
        """
        self.messenger.Debug("Computing error rates.")
        cursor = self.db.cursor()
        for ip, counter in self.counters.items():
            counter.ComputeErrors(cursor, self.job_id)
        cursor.close()
        self.completed = 1
        self.db.commit()


def AnalyseInputData(flows, bins, ip_pool=None):
    """
    Reads the flow data and analyses it
    :param flows:
    :return:
    """

    logging.info("Analysing input data")
    df = pd.read_json(flows)
    logging.info(f"Read {len(df):,} records from {flows}")

    # Analyse the input data to work out its shape. Stick it in a histogram and then create a reverse histogram
    # function which will allow us to generate random data of the same shape. Then generate a set of random data
    # and compare its properties with the real thing...

    logging.info("Computing IP address distribution")
    byte_hist = np.histogram(df['bytes'], bins=bins)
    inverse_hist = scipy.stats.rv_histogram(byte_hist)
    generated = inverse_hist.rvs(size=len(df['bytes']))
    logging.debug("Comparison of reverse histogram fitting")
    logging.debug("Measure     Real data   Simulated data")
    logging.debug(f"Mean:        {np.mean(df['bytes']):8.2f}         {np.mean(generated):8.2f}")
    logging.debug(f"Min:         {np.min(df['bytes']):8.2f}         {np.min(generated):8.2f}")
    logging.debug(f"Max:         {np.max(df['bytes']):8.2f}         {np.max(generated):8.2f}")
    logging.debug(f"Std:         {np.std(df['bytes']):8.2f}         {np.std(generated):8.2f}")
    logging.debug(f"Mean:        {np.median(df['bytes']):8.2f}         {np.median(generated):8.2f}")
    for p in [10, 25, 33, 50, 66, 75, 90, 95, 99]:
        logging.debug(f"{p}th %ile:   {np.percentile(df['bytes'], p):8.2f}         {np.percentile(generated, p):8.2f}")

    # Now get the distribution of IP addresses

    ip_dist = df.groupby(["ip"]).count()["bytes"]
    if ip_pool and ip_pool < len(ip_dist):
        logging.debug(f"Trimming IP pool to {ip_pool:,} random entries")
        ip_dist = ip_dist.sample(n=ip_pool)
    return inverse_hist, ip_dist.to_dict()


def PrimeFactors(n):
    """
    Compute the prime factors of n and return them as a dictionary. SO for n=100 would return {2:2, 5:2}
    :param n:
    :return: defaultdict where key=factor and value=order
    """
    f = collections.defaultdict(int)

    while n % 2 == 0:
        f[2] += 1
        n //= 2

    for i in range(3, int(math.sqrt(n))+1, 2):
        while n % i == 0:
            f[i] += 1
            n //= i

    if n > 1:
        f[n] += 1

    return f

def Aliasing(block_size, rates):
    """
    So we can count/estimate accurately, we need to have a block size which is an exact multiple of all the
    blocksizes to be tested. So we need to check that the block size's prime factors e match those of the test
    ratios
    """

    mpfs = collections.defaultdict(int) # Minimum required prime factors
    for r in rates:
        rpf = PrimeFactors(r)
        for factor, order in rpf.items():
            if order > mpfs[factor]:
                mpfs[factor] = order
    minval = 1
    for factor, order in mpfs.items():
        minval *= factor**order

    bpf = PrimeFactors(block_size)
    for factor, order in mpfs.items():
        if order > bpf[factor]:
            return minval

def Worker(args, job, ivh, ipmap):
    """
    Worker process for multiprocessing
    :return: 
    """
    with Sampler(args, job, ivh, ipmap) as s:
        s.Simulate()
        s.AnalyseResults()

if __name__ == '__main__':

    ap = argparse.ArgumentParser(description="Analyse sFlow data and then simulate it")
    ap.add_argument("--debug", action="store_true", help="Debug mode")
    ap.add_argument("--block-size", help="Size of data block", type=int, default=1000000)
    ap.add_argument("--block-count", help="Number of blocks to return per run", type=int, default=50000)
    ap.add_argument("--ip-pool", metavar="n", help="Maximum size of pool of IP addresses to use", default=1000)
    ap.add_argument("--runs", type=int, help="Number of runs to perform", default=20)
    ap.add_argument("--bins", type=int, help="Number of histogram bins for RV distribution", default=200)
    ap.add_argument("--rates", metavar="Sampling rates", type=int, action="append", nargs="+",
                    default=[50000, 25000, 10000, 5000, 2500, 1000, 250])
    ap.add_argument("--credentials", metavar="YAML file", help="File containing DB credentials",
                    default="credentials.yaml")
    ap.add_argument("flows", metavar="JSON file", help="Flow data")
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")

    try:
        minval = Aliasing(args.block_size, args.rates)
        if minval:
            logging.critical(f"Aliasing detected. Block size must be a multiple of {minval:,}")
        else:
            ivh, ipmap = AnalyseInputData(args.flows, args.bins, args.ip_pool)
            jobs = []
            for job in range(args.runs):
                p = multiprocessing.Process(target=Worker, args=(args, job, ivh, ipmap))
                jobs.append(p)
                p.start()

            for p in jobs:
                p.join()

            logging.info("Done!")
    except KeyboardInterrupt:
        print("Keyboard interrupt. Bye.")
