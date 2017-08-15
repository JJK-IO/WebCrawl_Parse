from sqlalchemy.orm import sessionmaker

from python.intel_parse.models import engine, Broker

Session = sessionmaker(bind=engine)
session = Session()

with open('workfile', 'w') as f:
    all_brokers = session.query(Broker).all()
    print(len(all_brokers))
    for broker in all_brokers:
        print(broker)
        for advertiser in broker.advertisers:
            print(advertiser)
            f.write("[ \"" + broker.name + "\", \"" + advertiser.name + "\", \"" + str(advertiser.count) + "\" ]")
            f.write("\n")
