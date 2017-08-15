import csv
from timeit import default_timer as timer


class Broker():
    def __init__(self, url):
        self.url = url
        self.advertisers = []

    def __repr__(self):
        return self.url

    def get_advert(self, url):
        for advert in self.advertisers:
            if advert.url == url:
                return advert
        return None


class Advertiser():
    def __init__(self, url):
        self.url = url
        self.count = 1

parse_file = "events.ra.csv"
file_mem = []

with open(parse_file, 'rt') as csvfile:
    reader = csv.reader(csvfile)
    for line in reader:
        file_mem.append(line[3])

print("Saved to mem")

check_loop = 100000
start_time = timer()
timing = []
parse = False
broker = None
brokers = []

for index, line in enumerate(file_mem):
    if index != 0 and index % check_loop == 0:
        time_passed = timer() - start_time
        timing.append(time_passed)
        print("Processed %s in: %s" % (check_loop, time_passed))
        print("Avg time: %s" % (sum(timing) / len(timing)))
        print("Current: %s" % index)
        print("")
        start_time = timer()
    if parse:
        if line != "clickbait.riskanalytics.com":
            if index == parse_past_line + 1:
                if line not in brokers:
                    brokers.append(Broker(line))
                broker = line
            elif broker is not None and index > parse_past_line + 1:
                for b in brokers:
                    if b.url == broker:
                        advert = b.get_advert(line)
                        if advert:
                            advert.count += 1
                        else:
                            b.advertisers.append(Advertiser(line))
        else:
            parse_past_line = index
    else:
        if line != "clickbait.riskanalytics.com":
            print("Found first Clickbait.")
            parse_past_line = index
            parse = True


with open('workfile2', 'w') as f:
    print("Brokers: %s" % len(brokers))
    for b in brokers:
        print(b)
        for advertiser in b.advertisers:
            f.write("[ \"" + b.url + "\", \"" + advertiser.url + "\", \"" + str(advertiser.count) + "\" ]")
            f.write("\n")
