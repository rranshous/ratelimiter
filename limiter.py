

# port of https://github.com/chriso/redback/blob/master/lib/advanced_structures/RateLimit.js

## an object backed by redis which
# is capable of tracking the rate of usage
# of a given action for a given unique user

# so this pattern seems to have a built in assumption
# that you are going to continue to add even if
# your count it too high. since it only removes history
# when you add, if you saturate the rate and than walk away
# for a min and come back, and ur data still says u've
# been saturating (since add hasn't cleared history)
# it seems the count method would need to be updated
# to also clear history

from time import time
import math


class RateLimiter(object):
    def __init__(self, redis_client,
                       base_key,
                       bucket_span = 600,
                       bucket_interval = 5,
                       subject_expire = 1200):

        self.rc = redis_client
        self.bucket_span = bucket_span
        self.bucket_interval = bucket_interval
        self.subject_expire = subject_expire
        self.base_key = base_key
        self.bucket_count = round(self.bucket_span / self.bucket_interval)

    def _get_bucket(self, _time=None):
        _time = _time or time()
        b = int(math.floor((_time % self.bucket_span) / self.bucket_interval))
        return b

    def add(self, subject, amt=1):
        bucket = self._get_bucket()
        subject = self.base_key + ':' + subject

        pipe = self.rc.pipeline()
        pipe.hincrby(subject, bucket, amt)
        self._clear_ahead(subject,bucket,pipe)
        pipe.expire(subject, self.subject_expire)
        pipe.execute()

        return True

    def count(self, subject, interval):
        bucket = self._get_bucket()
        count = math.floor(interval / self.bucket_interval)
        subject = self.base_key + ':' + subject

        pipe = self.rc.pipeline()

        # we are going to clear ahead any way
        self._clear_ahead(subject,bucket,pipe)

        pipe.hget(subject, bucket)
        bucket_c = bucket - 1
        for i in xrange(int(count - 1)):
            b = int((bucket_c + self.bucket_count) % self.bucket_count)
            pipe.hget(subject,b)
            bucket_c -= 1


        r = pipe.execute()
        c = sum(map(int,[_r for _r in r if _r]))
        return c

    def _clear_ahead(self,subject,bucket=None,pipe=None):
        if not bucket:
            bucket = self._get_bucket()
        given_pipe = pipe is not None
        if not pipe:
            pipe = self.rc.pipeline()
        pipe.hdel(subject, int((bucket + 1) % self.bucket_count))
        pipe.hdel(subject, int((bucket + 2) % self.bucket_count))
        if given_pipe:
            return pipe
        return pipe.execute()


