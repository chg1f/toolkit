#!python3
# encoding=utf-8
import logging
import asyncio
import os
import urllib
import urllib.parse

try:
    import aiohttp
    import aiohttp.client_exceptions
    import aiofile
except ImportError:
    print("python3 -m pip install aiohttp aiofile")


class Fetcher(object):
    def __init__(self, base_url, output_dir):
        self._session = aiohttp.ClientSession()
        assert base_url and base_url.endswith(".git/")
        self.base_url = base_url
        if not output_dir:
            parsed = urllib.parse.urlparse(base_url)
            output_dir = parsed.path
            if parsed.hostname:
                output_dir = parsed.hostname + output_dir
        self.output_dir = output_dir

    async def _fetch(self, path):
        remote = urllib.parse.urljoin(self.base_url, path)
        local = os.path.abspath(os.path.join(self.output_dir, path))
        os.makedirs(os.path.dirname(local), exist_ok=True)
        async with self._session.get(remote) as resp:
            try:
                resp.raise_for_status()
                async with aiofile.async_open(local, "wb") as fp:
                    async for chunk in resp.content.iter_chunked(1024):
                        await fp.write(chunk)
            except aiohttp.client_exceptions.ClientResponseError:
                logging.info(f"[{resp.status}]fetched {remote}")
                if resp.status in (404,):
                    return
                raise
            else:
                logging.info(f"[{resp.status}]fetched {remote} to {local}")

    async def fetch(self):
        await self._fetch("../.gitignore")
        if os.path.exists("../.gitignore"):
            async with aiofile.async_open("../.gitignore", "r") as fp:
                while True:
                    line = await fp.readline()
                    line = line.strip()
                    if not line:
                        break
                    try:
                        await self._fetch(f"../{line}")
                    except aiohttp.client_exceptions.ClientResponseError:
                        pass

        await self._fetch("config")
        await self._fetch("index")

        await self._fetch("logs/HEAD")
        await self._fetch("logs/refs/heads/master")
        await self._fetch("logs/refs/remotes/origin/master")

        await self._fetch("refs/heads/master")
        await self._fetch("refs/remotes/origin/master")

        objects = set()
        for basepath, _, filenames in os.walk("logs"):
            for filename in filenames:
                async with aiofile.async_open(os.path.join(basepath, filename)) as fp:
                    while True:
                        line = await fp.readline()
                        line = line.strip()
                        if not line:
                            break
                        parts = line.split()
                        objects.add(parts[0])
                        objects.add(parts[1])

        for object in objects:
            if object != "0000000000000000000000000000000000000000":
                await self._fetch(f"objects/{object[:2]}/{object[2:]}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str, help="https://example.com/.git/")
    parser.add_argument("--output", "-o", type=str, help="output path")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    loop = asyncio.get_event_loop()

    fetcher = Fetcher(args.path, args.output)
    loop.run_until_complete(fetcher.fetch())
