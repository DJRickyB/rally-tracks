import random
import os
import base64

class QueryParamSource:
    # We need to stick to the param source API
    # noinspection PyUnusedLocal
    def __init__(self, track, params, **kwargs):
        self._params = params
        self.infinite = True
        cwd = os.path.dirname(__file__)
        # The terms.txt file has been generated with:
        # sed -n '13~250p' [path_to_rally_data]/geonames/documents.json | shuf | sed -e "s/.*name\": \"//;s/\",.*$//" > terms.txt
        with open(os.path.join(cwd, "terms.txt"), "r") as ins:
            self.terms = [line.strip() for line in ins.readlines()]

    # We need to stick to the param source API
    # noinspection PyUnusedLocal
    def partition(self, partition_index, total_partitions):
        return self


class PureTermsQueryParamSource(QueryParamSource):
    def params(self):
        query_terms = list(self.terms)  # copy
        query_terms.append(str(random.randint(1, 100)))  # avoid caching
        result = {
            "body": {
                "query": {
                    "terms": {
                        "name.raw": query_terms
                    }
                }
            },
            "index": None
        }
        if "cache" in self._params:
            result["cache"] = self._params["cache"]

        return result

class PureTermsQueryParamSourceTraced(PureTermsQueryParamSource):
    # indeed a very dumb idea for a param-source but shows how traffic would look if rally had a switch for enabling
    # opentracing tooling for Elastic APM or Jaeger, if we wanted to debug a thing
    def params(self):
        result = super().params()
        trace_id = "%032x" % random.randrange(16**30)
        parent_id = "%016x" % random.randrange(16**30)
        trace_header = f"00-{trace_id}-{parent_id}-01"
        result["headers"] = {
            "traceparent": trace_header,
            "tracestate": f"myfakeapm-{base64.b64encode(parent_id.encode('ascii'))}"
        }
        result["opaque-id"] = trace_id
        return result

class FilteredTermsQueryParamSource(QueryParamSource):
    def params(self):
        query_terms = list(self.terms)  # copy
        query_terms.append(str(random.randint(1, 1000)))  # avoid caching
        result = {
            "body": {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "feature_class.raw": "T"
                                }
                            }
                        ],
                        "filter": [
                            {
                                "terms": {
                                    "name.raw": query_terms
                                }
                            }
                        ]
                    }
                }
            },
            "index": None
        }
        if "cache" in self._params:
            result["cache"] = self._params["cache"]

        return result


class ProhibitedTermsQueryParamSource(QueryParamSource):
    def params(self):
        query_terms = list(self.terms)  # copy
        query_terms.append(str(random.randint(1, 1000)))  # avoid caching
        result = {
            "body": {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "feature_class.raw": "A"
                                }
                            }
                        ],
                        "must_not": [
                            {
                                "terms": {
                                    "name.raw": query_terms
                                }
                            }
                        ]
                    }
                }
            },
            "index": None
        }
        if "cache" in self._params:
            result["cache"] = self._params["cache"]

        return result



def refresh(es, params):
    es.indices.refresh(index=params.get("index", "_all"))


def register(registry):
    registry.register_param_source("pure-terms-query-source", PureTermsQueryParamSource)
    registry.register_param_source("pure-terms-query-source-traced", PureTermsQueryParamSourceTraced)
    registry.register_param_source("filtered-terms-query-source", FilteredTermsQueryParamSource)
    registry.register_param_source("prohibited-terms-query-source", ProhibitedTermsQueryParamSource)
