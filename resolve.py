#!python3
# encoding=utf-8
import typing
import logging
import asyncio
import json

import aiodns


logger = logging.getLogger()


def _merge(hosts: typing.List[str], paths: typing.List[str]) -> typing.List[str]:
    hosts = hosts or []
    paths = paths or []
    results = set()
    for host in hosts:
        results.add(host)
    for path in paths:
        with open(path) as fp:
            for line in fp.readlines():
                results.add(line.strip())
    return list(results)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--host", nargs="*", help="resolve hosts")
    parser.add_argument("--hosts", nargs="*", help="resolve host text files")
    parser.add_argument(
        "--nameserver",
        nargs="*",
        help="resolve servers",
        default=["8.8.8.8", "223.5.5.5"],
    )
    parser.add_argument("--nameservers", nargs="*", help="resolve server text files")
    parser.add_argument(
        "--qtypes", nargs="*", help="resolve query types", default=["A", "CNAME"]
    )
    parser.add_argument("--verbose", action="store_true", help="verbose")
    parser.add_argument("--format", default="json", help="output format")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    hosts = _merge(hosts=args.host, paths=args.hosts)
    nameservers = _merge(hosts=args.nameserver, paths=args.nameservers)
    qtypes = list(set(t for t in args.qtypes if t in aiodns.query_type_map.keys()))
    loop = asyncio.get_event_loop()

    assert all(
        [t in aiodns.query_type_map.keys() for t in qtypes]
    ), "unexpect resolve query type"
    loop = asyncio.get_event_loop()
    resolver = aiodns.DNSResolver(nameservers=nameservers, loop=loop)

    temp = dict()
    for host in hosts:
        temp[host] = []
        for qtype in qtypes:
            temp[host].append(resolver.query(host=host, qtype=qtype))
    for host, futures in temp.items():
        try:
            loop.run_until_complete(asyncio.gather(*futures, loop=loop))
        except Exception as e:
            logging.error("{}: {!r}".format(host, e))
    loop.close()
    results: typing.List[typing.Dict] = []
    for host, futures in temp.items():
        for future in futures:
            if not future.done():
                continue
            try:
                result = future.result()
            except Exception as e:
                logging.error("{}: {!r}".format(host, e))
                continue
            if not isinstance(result, typing.Iterable):
                result = [result]
            for r in result:
                t = {"query": host}
                for k in r.__slots__:
                    t[k] = getattr(r, k)
                results.append(t)

    keys = list(set([k for r in results for k in r.keys()]))
    if args.format == "csv":
        print(";".join(keys))
    for line in results:
        if args.format == "csv":
            print(";".join([str(line[k]) if k in line else "" for k in keys]))
        else:
            print(json.dumps(line))
