FROM docker.io/wernight/phantomjs
MAINTAINER sqk_king@sina.com

ADD phantomjs_fetcher.js /opt/phantomjs_fetcher.js

ENTRYPOINT ["phantomjs", "--load-images=false", "--ssl-protocol=any", "--disk-cache=true", '/opt/phantomjs_fetcher.js', '25555']