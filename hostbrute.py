#!python3
# encoding=utf-8
import typing
import logging
import asyncio
import hashlib
import urllib.parse
import os.path
import json
import random
import string

import tld
import tld.exceptions
import bs4
import aiodns
import aiohttp


logger = logging.getLogger()
# session = aiohttp.ClientSession()


def _merge(targets=[], targets_files=[]):
    results = set()
    if targets:
        [results.add(t) for t in targets if t]
    if targets_files:
        for targets_file in targets_files:
            with open(targets_file) as fp:
                for line in fp.readlines():
                    line = line.strip()
                    if not line.startswith("#") and line:
                        results.add(line)
    return results


def randstr(min, max):
    # (int, int) -> str
    return "".join(
        [
            random.choice(string.ascii_letters + string.digits)
            for _ in range(random.randint(min, max))
        ]
    )


class HostBruter(object):
    def __init__(self, nameservers=None, loop=None, **kwargs):
        if not loop:
            loop = asyncio.get_event_loop()
        self._loop = loop

        # {(url, host): (status, title, hash)}
        self._cache = (
            {}
        )  # type: typing.Dict[typing.Tuple[str, str], typing.Optional[typing.Dict[str, typing.Union[int, str, typing.List[str]]]]]

        if not nameservers:
            nameservers = ["223.5.5.5"]
        aiohttp.resolver.aiodns_default = True
        self._resolver = aiohttp.AsyncResolver(nameservers=nameservers, loop=self._loop)
        self._connector = aiohttp.TCPConnector(
            ssl=False, resolver=self._resolver, **kwargs, loop=self._loop
        )
        # {url: aiohttp.ClientSession}
        self._sessions = {}  # type: typing.Dict[str, aiohttp.ClientSession]

    def _get_session(self, url):
        # (str, typing.Dict[str, any]) -> aiohttp.ClientSession
        if url not in self._sessions:
            self._sessions[url] = aiohttp.ClientSession(
                base_url=url, connector=self._connector, loop=self._loop
            )
        return self._sessions[url]

    async def close(self):
        for session in self._sessions.values():
            await session.close()

    def load(self, path=None):
        path = os.path.abspath(
            os.path.expandvars(
                os.path.expanduser(path if path else "./hostbrute.jsonl")
            )
        )
        with open(path) as fp:
            for line in fp.readlines():
                line = line.strip()
                if line:
                    c = json.loads(line)
                    self._cache[c.pop("url"), c.pop("host")] = c

    def dump(self, path=None):
        path = os.path.abspath(
            os.path.expandvars(
                os.path.expanduser(path if path else "./hostbrute.jsonl")
            )
        )
        with open(path, "a") as fp:
            for k, v in self._cache.items():
                if v:
                    v.update({"url": k[0], "host": k[1]})
                    fp.write(json.dumps(v, ensure_ascii=False) + "\n")

    # (base_url, host) -> (status, title, hash)
    async def _request(self, url, host):
        # (str, str) -> typing.Optional[typing.Tuple[int, str, str]]
        if (url, host) not in self._cache:
            session = self._get_session(url)
            try:
                path = urllib.parse.urlparse(url).path
                headers = {"Host": host} if host else {}
                if headers["Host"].startswith("@."):
                    headers["Host"] = randstr(1, 10) + host[2:]
                response = await session.get(path if path else "/", headers=headers)
                body = await response.text("utf-8")
                title = bs4.BeautifulSoup(body, "html.parser").find("title")
                self._cache[(url, host)] = {
                    "status": response.status,
                    "title": title.text if title else "",
                    "hash": hashlib.md5(body.encode("utf-8")).hexdigest(),
                }
            except Exception as e:
                self._cache[(url, host)] = None
                logger.debug(f"request {(url, host)} {e}")
        logger.debug(f"request {(url, host)} {self._cache[(url, host)]}")
        return self._cache[(url, host)]

    def _urlrelpace(self, url, host):
        # (str, str) -> str
        u = urllib.parse.urlparse(url)
        return urllib.parse.urlunparse(
            (
                u.scheme,
                host + ":" + str(u.port) if u.port else host,
                u.path,
                "",
                u.query,
                u.fragment,
            )
        )

    async def _resolve(self, url):
        chain = []
        try:
            u = tld.get_tld(url, as_object=True)
            assert isinstance(u, tld.Result)
            d = u.parsed_url.netloc
            if ":" in d:
                d, _, _ = d.rpartition(":")

            try:
                q = await self._resolver._resolver.query(d, "CNAME")
                if q and getattr(q, "cname"):
                    chain.append((self._urlrelpace(url, getattr(q, "cname")),))
                    c = await self._resolve(self._urlrelpace(url, getattr(q, "cname")))
                    chain.extend(c)
            except aiodns.error.DNSError:
                t = set()
                try:
                    [
                        t.add(self._urlrelpace(url, getattr(q, "host")))
                        for q in await self._resolver._resolver.query(d, "A")
                        if q and hasattr(q, "host")
                    ]
                except aiodns.error.DNSError:
                    pass
                try:
                    [
                        t.add(self._urlrelpace(url, getattr(q, "host")))
                        for q in await self._resolver._resolver.query(d, "AAAA")
                        if q and hasattr(q, "host")
                    ]
                except aiodns.error.DNSError:
                    pass
                chain.append(t)
        except tld.exceptions.TldDomainNotFound:
            chain.append([url])
        logger.debug(f"{url} via {chain}")
        return chain

    async def hostbrute(self, url, brutes):
        # (str, typing.List[str]) -> typing.List[typing.Dict]
        results = []  # type: typing.List[typing.Dict]

        try:
            u = tld.get_tld(url, as_object=True)
            assert isinstance(u, tld.Result)
            d = u.parsed_url.netloc
            if ":" in d:
                d, _, _ = d.rpartition(":")

            ori = await self._request(url, d)
            ori.update(
                {
                    "url": url,
                    "host": d,
                }
            )
            pan = await self._request(url, "@." + u.fld)
            pan.update(
                {
                    "url": url,
                    "host": "@." + u.fld,
                }
            )
        except tld.exceptions.TldDomainNotFound:
            ori = await self._request(url, "")
            ori.update(
                {
                    "url": url,
                    "host": "",
                }
            )
            p = "@." + randstr(1, 10) + ".com"
            pan = await self._request(url, p)
            pan.update(
                {
                    "url": url,
                    "host": p,
                }
            )

        chain = await self._resolve(url)
        for target in chain[-1]:
            for brute in brutes:
                t = await self._request(target, brute)
                if t:
                    t.update(
                        {
                            "target": target,
                            "url": url,
                            "host": brute,
                            "via": chain[:-1],
                        }
                    )
                    if t["status"] == ori["status"] and t["hash"] == ori["hash"]:
                        logger.debug(f"{brute} brute {ori} ori {t}")
                    elif t["status"] == pan["status"] and t["hash"] == pan["hash"]:
                        logger.debug(f"{brute} brute {pan} pan {t}")
                    else:
                        results.append(t)
                        logger.debug(f"{brute} brute {t}")
        return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--urls", nargs="*")
    parser.add_argument("--urls-files", nargs="*")
    parser.add_argument("--brutes", nargs="*")
    parser.add_argument("--brutes-files", nargs="*")
    parser.add_argument("--output")
    parser.add_argument("--cache")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger("aiohttp.client").setLevel(logging.DEBUG)

    urls = _merge(args.urls, args.urls_files)
    brutes = _merge(args.brutes, args.brutes_files)
    assert urls and brutes, (urls, brutes)

    loop = asyncio.get_event_loop()
    hostbruter = HostBruter(loop=loop)
    if args.cache:
        hostbruter.load(args.cache)
    try:
        tasks = []
        for url in urls:
            tasks.append(hostbruter.hostbrute(url, brutes))
        group = asyncio.gather(*tasks)
        loop.run_until_complete(group)
    finally:
        loop.run_until_complete(hostbruter.close())
    if args.output:
        hostbruter.dump(args.output)
