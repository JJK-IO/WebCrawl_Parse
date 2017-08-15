import sys

import sqlite3
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy import ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

# creator = lambda: sqlite3.connect('file:memdb1?mode=memory&cache=shared', uri=True)
# engine = create_engine('sqlite:///:memory:', creator=creator)
# engine = create_engine('sqlite:///file::memory:?cache=shared', echo=False)
# engine = create_engine(
#     'postgresql+psycopg2://intel_parse_u:herpderphippos!@/intel_parse?host=/run/postgresql',
#     echo=False
# )
Base = declarative_base()


class Broker(Base):
    __tablename__ = 'broker'

    id = Column(Integer, primary_key=True)
    name = Column(String)

    def __repr__(self):
        return "<Broker(name='%s')>" % (self.name)


class Advertiser(Base):
    __tablename__ = 'advertisers'

    id = Column(Integer, primary_key=True)
    name = Column(String, )
    count = Column(Integer)
    broker_id = Column(Integer, ForeignKey('broker.id'))

    broker = relationship("Broker", back_populates="advertisers")

    def __repr__(self):
        return "<Advertiser(name='%s', count='%s')>" % (self.name, self.count)


Broker.advertisers = relationship("Advertiser", order_by=Advertiser.id, back_populates="broker")
