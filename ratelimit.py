from flask import request, current_app
from flask.ext.cache import Cache
from datetime import datetime, timedelta
import functools, sha
from werkzeug.exceptions import HTTPException
from werkzeug.contrib.cache import MemcachedCache

class RatelimitException(HTTPException):
    code = 403
    description = 'Over limit, probeer zometeen nogmaals'


class ratelimit(object):
    "Instances of this class can be used as decorators"
    # This class is designed to be sub-classed
    minutes = 2 # The time period
    requests = 20 # Number of allowed requests in that time period

    prefix = 'rl-' # Prefix for memcache key

    def __init__(self, **options):
        for key, value in options.items():
            setattr(self, key, value)

    def __call__(self, fn):
        def wrapper(request, *args, **kwargs):
            return self.view_wrapper(request, fn, *args, **kwargs)
        functools.update_wrapper(wrapper, fn)
        return wrapper

    def __get__(self, obj, objtype):
        """Support instance methods."""
        return functools.partial(self.__call__, obj)

    def view_wrapper(self, inst, fn, *args, **kwargs):

        self.cache = current_app.cache

        if not self.should_ratelimit(request):
            return fn(request, *args, **kwargs)

        counters = self.get_counters(request)
        counts = [c for c in counters if c is not None] if counters is not None else []

        # Increment rate limiting counter
        self.cache_incr(self.current_key(request))

        print counts
        # Have they failed?
        if sum(counts) >= self.requests:
            return self.disallowed(request)

        return fn(request, *args, **kwargs)


    def cache_get_many(self, *keys):
        result = self.cache.get_many(*keys)
        return result

    def cache_incr(self, key):
        # memcache is only backend that can increment atomically
        try:
            # add first, to ensure the key exists
            self.cache._cache.add(key, '0', time=self.expire_after())
            self.cache._cache.incr(key)
        except AttributeError:
            value = self.cache.get(key)
            value = value if value is not None else 0
            self.cache.set(key, value + 1, self.expire_after())

    def should_ratelimit(self, request):
        return True

    def get_counters(self, request):
        keys = self.keys_to_check(request)
        return self.cache_get_many(*keys)

    def keys_to_check(self, request):
        extra = self.key_extra(request)
        now = datetime.now()
        return [
            '%s%s-%s' % (
                self.prefix,
                extra,
                (now - timedelta(minutes = minute)).strftime('%Y%m%d%H%M')
            ) for minute in range(self.minutes + 1)
        ]

    def current_key(self, request):
        return '%s%s-%s' % (
            self.prefix,
            self.key_extra(request),
            datetime.now().strftime('%Y%m%d%H%M')
        )

    def key_extra(self, request):
        # By default, their IP address is used
        return "%s_%s" % (request.remote_addr, request.path)

    def disallowed(self, request):
        "Over-ride this method if you want to log incidents"
        return RatelimitException()

    def expire_after(self):
        "Used for setting the memcached cache expiry"
        return (self.minutes) * 60

class ratelimit_post(ratelimit):
    "Rate limit POSTs - can be used to protect a login form"
    key_field = None # If provided, this POST var will affect the rate limit

    def should_ratelimit(self, request):
        return request.method == 'POST'

    def key_extra(self, request):
        # IP address and key_field (if it is set)
        extra = super(ratelimit_post, self).key_extra(request)
        if self.key_field:
            value = sha.new(request.POST.get(self.key_field, '')).hexdigest()
            extra += '-' + value
        return extra