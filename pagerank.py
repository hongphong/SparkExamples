#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'phongphamhong'
# !/usr/bin/python
#
# Copyright 2015 Phong Pham Hong <phongbro1805@gmail.com>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from operator import add

url_links = sc.parallelize([
    ("url_1", "url_4"),
    ("url_2", "url_1"),
    ("url_3", "url_2"),
    ("url_3", "url_1"),
    ("url_4", "url_3"),
    ("url_4", "url_1")

]).groupByKey().cache()

url_ranks = url_links.keys().distinct().map(lambda x: (x, 1.0))
url_ranks.collect()


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


for k in xrange(0, 10):
    url_ranks = url_links.join(url_ranks).flatMap(lambda x: computeContribs(x[1][0], x[1][1])).reduceByKey(
        add).mapValues(lambda x: 0.15 + 0.85 * x)
url_ranks.collect()
