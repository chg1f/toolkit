#!python3
# encoding=utf-8
import logging
import asyncio
import os
import urllib
import urllib.parse
import string

try:
    import aiohttp
    import aiohttp.client_exceptions
    import aiofile
except ImportError:
    print("python3 -m pip install aiohttp aiofile")
    raise


class Fetcher(object):
    def __init__(self, base_url, output_dir):
        self._fetched = set()

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
        if path in self._fetched:
            return
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
            finally:
                self._fetched.add(path)

    async def _fetch_object(self, obj: str):
        if (
            len(obj) == 40
            and all(o in string.ascii_lowercase + string.digits for o in obj)
            and obj != "0" * 40
        ):
            await self._fetch(f"objects/{obj[:2]}/{obj[2:]}")
        elif obj.startswith("refs: "):
            ref = obj.replace("refs: ", "")
            await self._fetch(ref)
            async with aiofile.async_open(
                os.path.join(self.output_dir, ref), "r"
            ) as fp:
                line = await fp.readline()
                if line:
                    line = line.strip()
                    assert isinstance(line, str)
                    await self._fetch_object(line)

    async def _readlines(self, path: str):
        if not os.path.exists(path):
            return
        async with aiofile.async_open(
            os.path.join(self.output_dir, path), "r"
        ) as fp:
            while True:
                line = await fp.readline()
                if not line:
                    return
                yield line

    async def fetch(self):
        fetch_list = [
            "HEAD",
            "AUTO_MERGE",
            "FETCH_HEAD",
            "ORIG_HEAD",
            "COMMIT_EDITMSG",
            "config",
            "description",
            # "hooks",
            "info/exclude",
            # "objects/info",
            # "objects/pack",
            "index",
            # "refs/heads",
            "refs/heads/master",
            "refs/heads/main",
            # "refs/tags",
            "refs/origin/master",
            "refs/origin/main",
            "logs/HEAD",
            "logs/refs/heads/master",
            "logs/refs/heads/main",
            "logs/refs/origin/master",
            "logs/refs/origin/main",
            "../.gitignore",
        ]
        for path in fetch_list:
            await self._fetch(path)

        async for line in self._readlines("../.gitignore"):
            assert isinstance(line, str)
            try:
                await self._fetch(f"../{line}")
            except aiohttp.client_exceptions.ClientResponseError:
                pass

        for path in ["HEAD", "AUTO_MERGE", "FETCH_HEAD", "ORIG_HEAD"]:
            # TODO:
            async for line in self._readlines(path):
                assert isinstance(line, str)
                await self._fetch_object(line.strip())

        for basepath, _, filenames in os.walk(os.path.join(self.output_dir, "refs")):
            for filename in filenames:
                await self._fetch_object(os.path.join(basepath, filename))

        for basepath, _, filenames in os.walk(os.path.join(self.output_dir, "logs")):
            for filename in filenames:
                async with aiofile.async_open(os.path.join(basepath, filename)) as fp:
                    while True:
                        line = await fp.readline()
                        line = line.strip()
                        if not line:
                            break
                        parts = line.split()
                        if len(parts) > 2:
                            assert isinstance(parts[0], str)
                            await self._fetch_object(parts[0])
                            assert isinstance(parts[1], str)
                            await self._fetch_object(parts[1])


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
