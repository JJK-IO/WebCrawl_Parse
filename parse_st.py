import csv
from timeit import default_timer as timer

from sqlalchemy import create_engine, func
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.exc import MultipleResultsFound

from python.intel_parse.models import Broker, Base, Advertiser

engine = create_engine('sqlite:///:memory:')
Base.metadata.create_all(engine)
Session = scoped_session(sessionmaker(bind=engine, autoflush=True))
session = Session()


def do_work(duple):
    advertiser_domain, broker_name = duple

    # exit signal
    if advertiser_domain is None:
        session.commit()
        session.close()
        return

    # work
    session.flush()
    t_broker = session.query(Broker).filter(Broker.name == broker_name).one_or_none()
    try:
        advertiser = session.query(Advertiser). \
            filter(Advertiser.name == advertiser_domain). \
            filter(Advertiser.broker_id == t_broker.id).one_or_none()
    except MultipleResultsFound:
        advertiser = session.query(Advertiser). \
            filter(Advertiser.name == advertiser_domain). \
            filter(Advertiser.broker_id == t_broker.id).all()[0]

    if advertiser is not None:
        advertiser.count += 1
    else:
        new_advertiser = Advertiser(
            name=advertiser_domain,
            count=1,
            broker_id=t_broker.id
        )
        session.add(new_advertiser)
    session.commit()


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
                s.delete(duplicate_adverts.all())
                s.commit()


if __name__ == "__main__":

    parse_file = 'events.ra.csv'

    # produce data
    with open(parse_file, 'rt') as csvfile:
        reader = csv.reader(csvfile)
        parse = False
        parse_past_line = -1
        broker = None
        start_time = timer()
        check_loop = 1000
        timing = []
        for index, line in enumerate(reader):
            if index != 0 and index % check_loop == 0:
                time_passed = timer() - start_time
                timing.append(time_passed)
                print("Processed %s in: %s" % (check_loop, time_passed))
                print("Avg time: %s" % (sum(timing)/len(timing)))
                print("Current: %s" % index)
                print("")
                start_time = timer()
            if parse:
                if line[3] != "clickbait.riskanalytics.com":
                    if index == parse_past_line + 1:
                        broker = session.query(Broker).filter(Broker.name == line[3]).one_or_none()
                        if broker is None:
                            print("Creating new broker")
                            broker = Broker(name=line[3])
                            session.add(broker)
                            session.commit()
                            print(broker)
                    elif broker is not None and index > parse_past_line + 1:
                        do_work((line[3], broker.name))
                else:
                    parse_past_line = index
            else:
                if line[3] != "clickbait.riskanalytics.com":
                    print("Found first Clickbait.")
                    parse_past_line = index
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

    exit(0)
