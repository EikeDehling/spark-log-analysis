from pyspark import SparkContext
from operator import itemgetter
import re

sc = SparkContext('local[*]', 'Top Urls')

input = sc.textFile('access_log')

print("Lines : %s" % input.count())

# Apache log format:
# 127.0.0.1 (-) - - [05/Oct/2017:22:23:38 +0000] "GET / HTTP/1.1" 400 58640 "-" "Python-urllib/2.7"
log_regex = r'^.* - - \[.*\] "(.*)" \d+ [0-9-]+ ".*" ".*"$'

top_urls = input \
    .map(lambda line: re.match(log_regex, line)) \
    .filter(lambda match: match != None) \
    .map(lambda match: match.groups()[0]) \
    .map(lambda request: request.split()[1]) \
    .countByValue()

for url, count in sorted(top_urls.iteritems(), key=itemgetter(1), reverse=True)[:10]:
    print('{0: >32} => {1: >8}'.format(url, count))
