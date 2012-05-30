"Database cache backend."
import base64
from datetime import timedelta

try:
    import cPickle as pickle
except ImportError:
    import pickle

from django.core.cache.backends.base import BaseCache
from django.db import connections, router, transaction, DatabaseError, models
from django.db.transaction import force_managed
from django.utils import timezone

def create_cache_model(table):
    """
    This function will create a new cache table model to use for caching. The
    model is created dynamically, and isn't part of app-loaded Django models.
    """
    class CacheEntry(models.Model):
        cache_key = models.CharField(max_length=255, unique=True, primary_key=True)
        value = models.TextField()
        expires = models.DateTimeField(db_index=True)

        class Meta:
            db_table = table
            verbose_name = 'cache entry'
            verbose_name_plural = 'cache entries'
            # We need to be able to create multiple different instances of
            # this same class, and we don't want to leak entries into the
            # app-cache. This model must not be part of the app-cache also
            # because get_models() must not list any CacheEntry classes. So
            # use this internal flag to skip this class totally.
            _skip_app_cache = True

    opts = CacheEntry._meta
    opts.app_label = 'django_cache'
    opts.module_name = 'cacheentry'
    return CacheEntry
    

class BaseDatabaseCache(BaseCache):
    def __init__(self, table, params):
        BaseCache.__init__(self, params)
        self.cache_model_class = create_cache_model(table)
        self.objects = self.cache_model_class.objects

class DatabaseCache(BaseDatabaseCache):

    def get(self, key, default=None, version=None):
        key = self.make_key(key, version=version)
        db = router.db_for_write(self.cache_model_class)
        with force_managed(using=db):
            self.validate_key(key)
            try:
                obj = self.objects.using(db).get(cache_key=key)
            except self.cache_model_class.DoesNotExist:
                return default
            now = timezone.now()
            if obj.expires < now:
                obj.delete()
                return default
            # Note: we must commit_unless_managed even for read-operations to
            # avoid transaction leaks.
            value = connections[db].ops.process_clob(obj.value)
            return pickle.loads(base64.decodestring(value))

    def set(self, key, value, timeout=None, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        self._base_set('set', key, value, timeout)

    def add(self, key, value, timeout=None, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        return self._base_set('add', key, value, timeout)

    def _base_set(self, mode, key, value, timeout=None):
        db = router.db_for_write(self.cache_model_class)
        try:
            with force_managed(using=db):
                if timeout is None:
                    timeout = self.default_timeout
                now = timezone.now()
                now = now.replace(microsecond=0)
                exp = now + timedelta(seconds=timeout)
                num = self.objects.using(db).count()
                if num > self._max_entries:
                    self._cull(db, now)
                pickled = pickle.dumps(value, pickle.HIGHEST_PROTOCOL)
                encoded = base64.encodestring(pickled).strip()
                try:
                    val = self.objects.using(db).values_list(
                        'expires').get(cache_key=key)
                    if mode == 'set' or (mode == 'add' and val[0] < now):
                        obj = self.cache_model_class(expires=exp, value=encoded,
                                                     cache_key=key)
                        obj.save(using=db, force_update=True)
                    else:
                        return False
                except self.cache_model_class.DoesNotExist:
                    self.objects.using(db).create(cache_key=key, expires=exp,
                                                  value=encoded)
        except DatabaseError:
            return False
        return True

    def delete(self, key, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        db = router.db_for_write(self.cache_model_class)
        with force_managed(using=db):
            self.objects.using(db).filter(cache_key=key).delete()

    def has_key(self, key, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        with force_managed(using=db):
            now = timezone.now()
            now = now.replace(microsecond=0)
            return self.objects.using(db).filter(cache_key=key, expires__gt=now).exists()

    def _cull(self, db, now):
        if self._cull_frequency == 0:
            # cull might be used inside other dbcache operations possibly already
            # doing commits themselves - so do not commit in clear.
            self.clear()
        else:
            # When USE_TZ is True, 'now' will be an aware datetime in UTC.
            self.objects.using(db).filter(expires__lt=now).delete()
            num = self.objects.using(db).count()
            if num > self._max_entries:
                cull_num = num / self._cull_frequency
                limit = self.objects.using(db).values_list(
                    'cache_key').order_by('cache_key')[cull_num][0]
                self.objects.using(db).filter(cache_key__lt=limit).delete()

    def clear(self):
        db = router.db_for_write(self.cache_model_class)
        with force_managed(using=db):
            self.objects.using(db).delete()

# For backwards compatibility
class CacheClass(DatabaseCache):
    pass
