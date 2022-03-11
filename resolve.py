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


async def _query(resolver: aiodns.DNSResolver, host: str, qtype: str):
    result = {"query": host, "query_type": qtype}
    try:
        temp = await resolver.query(host=host, qtype=qtype)
        if not isinstance(temp, typing.Iterable):
            temp = [temp]
        for t in temp:
            for k in t.__slots__:
                if k not in result:
                    result[k] = list() # type: ignore
                result[k].append(getattr(t, k)) # type: ignore
    except asyncio.CancelledError:
        raise
    except aiodns.error.DNSError as e:
        logging.error("{} query {}: {!r}".format(host, qtype, e))
    except Exception as e:
        logging.exception("{} query {}: {!r}".format(host, qtype, e))
        raise
    return result


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
    parser.add_argument("--output", default="stdout", help="output path")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    hosts = _merge(hosts=args.host, paths=args.hosts)
    nameservers = _merge(hosts=args.nameserver, paths=args.nameservers)
    qtypes = list(set(t for t in args.qtypes if t in aiodns.query_type_map.keys()))
    assert all(
        [t in aiodns.query_type_map.keys() for t in qtypes]
    ), "unexpect resolve query type"

    output = None
    if args.output != "stdout":
        output = open(args.output, "a")

    loop = asyncio.get_event_loop()
    resolver = aiodns.DNSResolver(nameservers=nameservers, loop=loop)

    tasks = []
    for host in hosts:
        for qtype in qtypes:
            tasks.append(
                loop.create_task(_query(resolver=resolver, host=host, qtype=qtype))
            )
    try:
        loop.run_until_complete(asyncio.gather(*tasks))
    finally:
        loop.close()
    for t in tasks:
        if not t.done():
            continue
        line = json.dumps(t.result())
        if output:
            output.write(line + "\n")
        else:
            print(line)
