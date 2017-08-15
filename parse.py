import csv
import itertools
import time
import psutil

from timeit import default_timer as timer
from multiprocessing import Process, Manager

from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import MultipleResultsFound

from python.intel_parse.models import Broker, Advertiser, Base

# Setup Database
engine = create_engine('sqlite:///db1', echo=False)
Session = scoped_session(sessionmaker(bind=engine, autoflush=True))
Base.metadata.create_all(engine)


def do_work(in_queue):
    # creator = lambda: sqlite3.connect('file:memdb1?mode=memory&cache=shared', uri=True)
    # t_engine = create_engine('sqlite:///:memory:', creator=creator)
    # t_engine = create_engine('sqlite:///file::memory:?cache=shared', echo=False)
    # Base.metadata.create_all(t_engine)
    # t_engine = create_engine(
    #     'postgresql+psycopg2://intel_parse_u:herpderphippos!@/intel_parse?host=/run/postgresql',
    #     echo=False
    # )
    t_session = Session()

    count = 0
    while True:
        item = in_queue.get()
        advertiser_domain, broker_id = item
        # exit signal
        if advertiser_domain is None:
            t_session.commit()
            return

        # work
        try:
            advertiser = t_session.query(Advertiser). \
                filter(Advertiser.name == advertiser_domain). \
                filter(Advertiser.broker_id == broker_id).one_or_none()
        except MultipleResultsFound:
            advertiser = t_session.query(Advertiser). \
                filter(Advertiser.name == advertiser_domain). \
                filter(Advertiser.broker_id == broker_id).all()[0]

        if advertiser is not None:
            advertiser.count += 1
        else:
            new_advertiser = Advertiser(
                name=advertiser_domain,
                count=1,
                broker_id=broker_id
            )
            t_session.add(new_advertiser)
        try:
            if count > 1000:
                t_session.commit()
                count = 0
            else:
                t_session.flush()
                count += 1
        except:
            time.sleep(0.0001)
            t_session.rollback()
            if count > 100:
                t_session.commit()
                count = 0
            else:
                t_session.flush()
                count += 1


def clean_advertisers(s):
    all_brokers = s.query(Broker).all()
    for x, cur_broker in enumerate(all_brokers):
        print("broker %s of %s" % (x, len(all_brokers)))
        related_advertiser = s.query(Advertiser).filter(Advertiser.broker_id == cur_broker.id).all()
        for advertiser in related_advertiser:
            try:
                unique_advert = s.query(Advertiser). \
                    filter_by(name=advertiser.name). \
                    filter_by(broker_id=cur_broker.id).one_or_none()
            except MultipleResultsFound:
                duplicate_adverts = s.query(Advertiser). \
                    filter_by(name=advertiser.name). \
                    filter_by(broker_id=cur_broker.id)
                total_count = duplicate_adverts.with_entities(func.sum(Advertiser.count)).scalar()
                combined_advert = Advertiser(
                    name=duplicate_adverts.all()[0].name,
                    count=total_count,
                    broker_id=cur_broker.id
                )
                s.add(combined_advert)
                for advert in duplicate_adverts.all():
                    s.delete(advert)
                s.commit()


if __name__ == "__main__":
    session = Session()
    parse_file = 'events.ra.csv'
    num_workers = 4
    manager = Manager()
    work = manager.Queue(num_workers)

    # start for workers
    pool = []
    for i in range(num_workers):
        print("spawning process")
        p = Process(target=do_work, args=(work,))
        p.start()
        pool.append(p)

    # produce data
    with open(parse_file, 'rt') as csvfile:
        check_loop = 10000
        timing = []
        reader = csv.reader(csvfile)
        parse = False
        parse_past_line = -1
        broker = None
        iters = itertools.chain(reader, (None,) * num_workers)
        start_time = timer()
        for stuff in enumerate(iters):
            if stuff[0] != 0 and stuff[0] % check_loop == 0:
                time_passed = timer() - start_time
                timing.append(time_passed)
                print("Processed %s in: %s" % (check_loop, time_passed))
                print("Avg time: %s" % (sum(timing)/len(timing)))
                print("Current: %s" % stuff[0])
                print("Queue Size: %s" % work.qsize())
                print("")
                start_time = timer()
            if parse:
                try:
                    if stuff[1][3] != "clickbait.riskanalytics.com":
                        if stuff[0] == parse_past_line + 1:
                            broker = session.query(Broker).filter(Broker.name == stuff[1][3]).one_or_none()
                            if broker is None:
                                broker = Broker(name=stuff[1][3])
                                session.add(broker)
                                session.commit()
                                print("Creating Broker: %s" % broker)
                        elif stuff[0] > parse_past_line + 1:
                            work.put((stuff[1][3], broker.id))
                    else:
                        parse_past_line = stuff[0]
                except TypeError:
                    work.put((None, None))
            else:
                if stuff[1][3] != "clickbait.riskanalytics.com":
                    print("Found first Clickbait.")
                    parse_past_line = stuff[0]
                    parse = True

    session.commit()
    clean_advertisers(session)

    print(len(session.query(Broker).all()))
    print("done with db")

    with open('workfile', 'w') as f:
        all_brokers = session.query(Broker).all()
        print(len(all_brokers))
        for broker in all_brokers:
            print(broker)
            for advertiser in broker.advertisers:
                print(advertiser)
                f.write("[ \"" + broker.name + "\", \"" + advertiser.name + "\", " + str(advertiser.count) + " ]")
                f.write("\n")

    for p in pool:
        p.join()

    exit(0)
