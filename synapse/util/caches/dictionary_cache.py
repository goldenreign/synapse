# -*- coding: utf-8 -*-
# Copyright 2015, 2016 OpenMarket Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from synapse.util.caches.lrucache import LruCache
from collections import namedtuple
from . import register_cache
import threading
import logging


logger = logging.getLogger(__name__)


class DictionaryEntry(namedtuple("DictionaryEntry", ("full", "known_absent", "value"))):
    """Returned when getting an entry from the cache

    Attributes:
        full (bool): Whether the cache has the full or dict or just some keys.
            If not full then not all requested keys will necessarily be present
            in `value`
        known_absent (set): Keys that were looked up in the dict and were not
            there.
        value (dict): The full or partial dict value
    """
    def __len__(self):
        return len(self.value)


class DictionaryCache(object):
    """Caches key -> dictionary lookups, supporting caching partial dicts, i.e.
    fetching a subset of dictionary keys for a particular key.
    """

    def __init__(self, name, max_entries=1000):
        self.cache = LruCache(max_size=max_entries, size_callback=len)

        self.name = name
        self.sequence = 0
        self.thread = None
        # caches_by_name[name] = self.cache

        class Sentinel(object):
            __slots__ = []

        self.sentinel = Sentinel()
        self.metrics = register_cache(name, self.cache)

    def check_thread(self):
        expected_thread = self.thread
        if expected_thread is None:
            self.thread = threading.current_thread()
        else:
            if expected_thread is not threading.current_thread():
                raise ValueError(
                    "Cache objects can only be accessed from the main thread"
                )

    def get(self, key, dict_keys=None):
        """Fetch an entry out of the cache

        Args:
            key
            dict_key(list): If given a set of keys then return only those keys
                that exist in the cache.

        Returns:
            DictionaryEntry
        """
        entry = self.cache.get(key, self.sentinel)
        if entry is not self.sentinel:
            self.metrics.inc_hits()

            if dict_keys is None:
                return DictionaryEntry(entry.full, entry.known_absent, dict(entry.value))
            else:
                return DictionaryEntry(entry.full, entry.known_absent, {
                    k: entry.value[k]
                    for k in dict_keys
                    if k in entry.value
                })

        self.metrics.inc_misses()
        return DictionaryEntry(False, set(), {})

    def invalidate(self, key):
        self.check_thread()

        # Increment the sequence number so that any SELECT statements that
        # raced with the INSERT don't update the cache (SYN-369)
        self.sequence += 1
        self.cache.pop(key, None)

    def invalidate_all(self):
        self.check_thread()
        self.sequence += 1
        self.cache.clear()

    def update(self, sequence, key, value, full=False, known_absent=None):
        """Updates the entry in the cache

        Args:
            sequence
            key
            value (dict): The value to update the cache with.
            full (bool): Whether the given value is the full dict, or just a
                partial subset there of. If not full then any existing entries
                for the key will be updated.
            known_absent (set): Set of keys that we know don't exist in the full
                dict.
        """
        self.check_thread()
        if self.sequence == sequence:
            # Only update the cache if the caches sequence number matches the
            # number that the cache had before the SELECT was started (SYN-369)
            if known_absent is None:
                known_absent = set()
            if full:
                self._insert(key, value, known_absent)
            else:
                self._update_or_insert(key, value, known_absent)

    def _update_or_insert(self, key, value, known_absent):
        entry = self.cache.setdefault(key, DictionaryEntry(False, set(), {}))
        entry.value.update(value)
        entry.known_absent.update(known_absent)

    def _insert(self, key, value, known_absent):
        self.cache[key] = DictionaryEntry(True, known_absent, value)
